"""
手动回填市场整体估值数据
用法: python manage.py backfill_market_data --start 20250102 --end 20260417
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from datetime import datetime, timedelta, date
import pandas as pd
import akshare as ak
import logging
import requests
import zipfile
import io
import os
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = '回填市场整体估值数据'

    def add_arguments(self, parser):
        parser.add_argument('--start', type=str, required=True, help='开始日期 YYYYMMDD')
        parser.add_argument('--end', type=str, required=True, help='结束日期 YYYYMMDD')
        parser.add_argument('--dry-run', action='store_true', help='只打印不保存')

    def handle(self, *args, **options):
        start_date = datetime.strptime(options['start'], '%Y%m%d').date()
        end_date = datetime.strptime(options['end'], '%Y%m%d').date()
        dry_run = options['dry_run']

        self.stdout.write(f'回填范围: {start_date} ~ {end_date}')
        if dry_run:
            self.stdout.write(self.style.WARNING('[Dry Run 模式]'))

        # 生成交易日列表
        date_list = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # Mon-Fri
                date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)

        self.stdout.write(f'共 {len(date_list)} 个交易日')

        # 导入需要在Django setup之后
        import django
        django.setup()
        from fire_100UpPlan.models import MarketValuation
        from django.db.models import Min, Max

        # 过滤已存在的日期
        existing = set(MarketValuation.objects.filter(
            date__gte=start_date, date__lte=end_date
        ).values_list('date', flat=True))

        to_fill = [d for d in date_list if datetime.strptime(d, '%Y%m%d').date() not in existing]
        self.stdout.write(f'需要回填: {len(to_fill)} 天')
        if not to_fill:
            self.stdout.write(self.style.SUCCESS('没有需要回填的日期'))
            return

        # 获取上证指数日线数据
        self.stdout.write('获取上证指数日线数据...')
        sh_index = ak.stock_zh_index_daily(symbol='sh000001')
        sh_index['date'] = pd.to_datetime(sh_index['date']).dt.date

        # 获取PE和PB数据（12年）
        self.stdout.write('获取PE/PB数据...')
        sh_pe = ak.stock_market_pe_lg(symbol='上证')
        sh_pe['日期'] = pd.to_datetime(sh_pe['日期']).dt.date

        sh_pb = ak.stock_market_pb_lg(symbol='上证')
        sh_pb['日期'] = pd.to_datetime(sh_pb['日期']).dt.date

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        success = 0
        fail = 0
        errors = []

        for today_str in tqdm(to_fill, desc='回填进度'):
            today_date = datetime.strptime(today_str, '%Y%m%d').date()

            # 跳过国庆、春节等长假期（指数数据可能没有）
            idx_row = sh_index[sh_index['date'] == today_date]
            if idx_row.empty:
                logger.warning(f'{today_str} 没有上证指数数据，跳过')
                fail += 1
                errors.append(f'{today_str}: 无指数数据')
                continue

            latest_sh_index_val = idx_row['close'].iloc[0]

            # 获取12年PE/PB序列
            twelve_years_ago = pd.Timestamp(today_date) - pd.DateOffset(years=12)
            pe_mask = sh_pe['日期'] >= twelve_years_ago.to_pydatetime().date()
            pb_mask = sh_pb['日期'] >= twelve_years_ago.to_pydatetime().date()
            pe_series = sh_pe.loc[pe_mask, '平均市盈率']
            pb_series = sh_pb.loc[pb_mask, '市净率']

            if pe_series.empty or pb_series.empty:
                logger.warning(f'{today_str} PE/PB数据不足')
                fail += 1
                errors.append(f'{today_str}: PE/PB数据为空')
                continue

            current_sh_pe = float(pe_series.iloc[-1])
            current_sh_pb = float(pb_series.iloc[-1])

            # 计算Rank百分位
            sh_pe_rank = len(pe_series[pe_series <= current_sh_pe]) / len(pe_series) * 100
            sh_pb_rank = len(pb_series[pb_series <= current_sh_pb]) / len(pb_series) * 100

            # 获取历史极值
            market_stats = MarketValuation.objects.aggregate(
                min_pe=Min('pe_ratio'),
                max_pe=Max('pe_ratio'),
                min_pb=Min('pb_ratio'),
                max_pb=Max('pb_ratio')
            )
            history_pe_min = min(market_stats['min_pe'] or 11.05, 11.05)
            history_pe_max = max(market_stats['max_pe'] or 29.95, 29.95)
            history_pb_min = min(market_stats['min_pb'] or 1.25, 1.25)
            history_pb_max = max(market_stats['max_pb'] or 3.52, 3.52)

            # 下载CSI估值数据
            today_fmt = today_date.strftime('%Y%m%d')
            url = f'https://oss-ch.csindex.com.cn/dl_resource/industry_pe/bk{today_fmt}.zip'

            try:
                resp = requests.get(url, headers=headers, timeout=15)
                if resp.status_code != 200:
                    raise Exception(f'HTTP {resp.status_code}')
            except Exception as e:
                logger.warning(f'{today_str} CSI估值数据下载失败: {e}，使用PE/PB数据估算')
                # 估算PE/PB区间百分位
                pe_range_pct = 0 if current_sh_pe <= history_pe_min else (
                    100 if current_sh_pe >= history_pe_max else
                    (current_sh_pe - history_pe_min) / (history_pe_max - history_pe_min) * 100
                )
                pb_range_pct = 0 if current_sh_pb <= history_pb_min else (
                    100 if current_sh_pb >= history_pb_max else
                    (current_sh_pb - history_pb_min) / (history_pb_max - history_pb_min) * 100
                )
                pe_ttm_ratio = current_sh_pe  # 用静态PE近似
                pe_ratio = current_sh_pe
                pb_ratio = current_sh_pb
                dividend_ratio = None
                stock_count = None
                total_volume = None
                total_amount = None
                market_temperature = None
            else:
                # 解析ZIP
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    xls_name = zf.namelist()[0]
                    xls_path = f'/tmp/bk_{today_fmt}.xls'
                    with open(xls_path, 'wb') as f:
                        f.write(zf.read(xls_name))

                    df_static = pd.read_excel(xls_path, sheet_name='板块静态市盈率')
                    market_row = df_static[df_static['板块名称'] == '沪深市场'].iloc[0]
                    pe_ratio = round(float(market_row['最新静态\n市盈率']), 2)

                    df_pe_ttm = pd.read_excel(xls_path, sheet_name='板块滚动市盈率')
                    market_row = df_pe_ttm[df_pe_ttm['板块名称'] == '沪深市场'].iloc[0]
                    pe_ttm_ratio = round(float(market_row['最新滚动\n市盈率']), 2)

                    df_pb = pd.read_excel(xls_path, sheet_name='板块市净率')
                    market_row = df_pb[df_pb['板块名称'] == '沪深市场'].iloc[0]
                    pb_ratio = round(float(market_row['最新市净率']), 2)

                    pe_range_pct = 0 if pe_ratio <= history_pe_min else (
                        100 if pe_ratio >= history_pe_max else
                        (pe_ratio - history_pe_min) / (history_pe_max - history_pe_min) * 100
                    )
                    pb_range_pct = 0 if pb_ratio <= history_pb_min else (
                        100 if pb_ratio >= history_pb_max else
                        (pb_ratio - history_pb_min) / (history_pb_max - history_pb_min) * 100
                    )

                    # 股息率从PB推算
                    dividend_ratio = round(1 / pb_ratio * 100, 2) if pb_ratio > 0 else None
                    stock_count = int(market_row.get('股票只数', 0)) or None

                    total_volume = None
                    total_amount = None
                    market_temperature = None

                try:
                    os.remove(xls_path)
                except:
                    pass

            # 跳过周末/节假日
            if today_date.weekday() >= 5:
                continue

            if dry_run:
                self.stdout.write(f'  [Dry] {today_str}: pe={pe_ratio}, pb={pb_ratio}, sh={latest_sh_index_val}')
                success += 1
                continue

            # 保存到数据库
            try:
                with transaction.atomic():
                    mv, created = MarketValuation.objects.update_or_create(
                        date=today_date,
                        sector_name='沪深市场',
                        defaults={
                            'pe_ratio': pe_ratio,
                            'pe_range_percentile': round(pe_range_pct, 2),
                            'pe_rank_percentile': round(sh_pe_rank, 2),
                            'pe_ttm_ratio': pe_ttm_ratio,
                            'pb_ratio': pb_ratio,
                            'pb_range_percentile': round(pb_range_pct, 2),
                            'pb_rank_percentile': round(sh_pb_rank, 2),
                            'dividend_ratio': dividend_ratio,
                            'stock_count': stock_count,
                            'total_volume': total_volume,
                            'total_amount': total_amount,
                            'market_temperature': market_temperature,
                            'sh_index': latest_sh_index_val,
                            'sh_pe_rank_percentile': round(sh_pe_rank, 2),
                            'sh_pb_rank_percentile': round(sh_pb_rank, 2),
                        }
                    )
                success += 1
            except Exception as e:
                fail += 1
                errors.append(f'{today_str}: {str(e)}')
                logger.error(f'{today_str} 保存失败: {e}')

        self.stdout.write(self.style.SUCCESS(f'\n完成: 成功 {success}, 失败 {fail}'))
        if errors:
            for err in errors[:10]:
                self.stdout.write(self.style.ERROR(f'  {err}'))
