import logging
from celery import shared_task
from django.core.mail import EmailMessage

logger = logging.getLogger(__name__)

@shared_task
def send_activation_email(subject, message, from_email, recipient_list, bcc_list):
    try:
        email = EmailMessage(
            subject,
            message,
            from_email,
            recipient_list,
            bcc=bcc_list
        )
        sent = email.send(fail_silently=False)
        
        if sent == 1:
            logger.info(f"Successfully sent activation email to {recipient_list[0]} and bcc to {bcc_list[0]}")
            return True
        else:
            logger.warning(f"Failed to send activation email to {recipient_list[0]}")
            return False
    except Exception as e:
        logger.error(f"Failed to send activation email: {str(e)}")
        return False

    
