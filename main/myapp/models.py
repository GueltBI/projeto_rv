from django.contrib.auth.models import AbstractUser
from django.db import models

class CustomUser(AbstractUser):
    groups = models.ManyToManyField(
        'auth.Group',
        related_name='custom_users',  # Novo related_name
        blank=True
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        related_name='custom_users_permissions',  # Novo related_name
        blank=True
    )