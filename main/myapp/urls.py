from django.urls import path
from .views import register_view, login_view, logout_view, home_view, protected_view, info_view

urlpatterns = [
    path('register/', register_view, name='register'),
    path('login/', login_view, name='login'),
    path('logout/', logout_view, name='logout'),
    path('info/', info_view, name='info_view'),
    path('protected/', protected_view, name='protected_view'),  # Corrigido para 'protected_view'
    path('', login_view, name='login'),
]