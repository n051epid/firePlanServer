from rest_framework import permissions
from rest_framework.exceptions import PermissionDenied
from .models import Membership
from django.utils import timezone

class MembershipPermission(permissions.BasePermission):
    message = "You do not have the required membership to access this feature."

    def __init__(self, allowed_memberships=None):
        self.allowed_memberships = allowed_memberships or ['Trial', 'Basic Monthly', 'Premium Monthly']

    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            raise PermissionDenied("Authentication is required to access this feature.")
        
        try:
            membership = Membership.objects.get(user=request.user, is_active=True)
            
            if membership.membership_type.name not in self.allowed_memberships:
                raise PermissionDenied(f"Your current membership ({membership.membership_type.name}) does not have access to this feature.")
            
            if membership.end_date and membership.end_date <= timezone.now():
                raise PermissionDenied("Your membership has expired. Please renew to access this feature.")
            
            return True
        except Membership.DoesNotExist:
            raise PermissionDenied("You do not have an active membership. Please subscribe to access this feature.")
