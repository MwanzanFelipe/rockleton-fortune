from django.conf.urls import *
from . import views
from . import z_queries
from zillions import settings

from django.contrib.auth.views import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth import views as auth_views
from django.views.static import serve

from django.db import models

from .views import *
from .z_queries import *

urlpatterns = [
    # Show index page
    url(r'^$', login_required(views.index), name='index'),
    
    # Show list of transactions
    url(r'transactions/$', login_required(views.transaction_list), name = 'transaction_list'),
    
    # Add new transactions
    url(r'transactions/new/$', login_required(AddTransactionView.as_view()), name = 'AddTransactionView'),
    # Edit transactions
    url(r'^transactions/edit/(?P<pk>\d+)/$', login_required(UpdateTransactionView.as_view()), name= 'UpdateTransactionView'),
    
    #FUTURE: There's got to be a better way to handle this 3 part import workflow
    #Point to the file to import
    url(r'transactions/import/$', login_required(views.import_transactions), name = 'transaction_import_list'),
    #Select transactions to import as new or to replace existing
    url(r'transactions/import/input/$', login_required(views.import_transaction_input), name = 'transaction_import_input'),
    #Enter the percentage per transaction
    url(r'transactions/import/save/$', login_required(views.import_transaction_save), name = 'transaction_import_save'),
    
    # Export transactions to csv for Google Sheets
    url(r'transactions/csv/$', login_required(views.export_csv), name = 'export_csv'),
    
    # Edit the weekly/monthly allocation per secondary transaction
    url(r'budget/edit/$', login_required(views.budget_edit), name = 'budget_edit'),
    
    #Fetch the weekly spend summary per secondary and primary categories
    url('ajax/budget/$', login_required(z_queries.q_budget_view_json), name='q_budget_view_json'),
    #Template to show weekly spend summary per secondary and primary categories
    url(r'budget/$', login_required(views.budget_view), name = 'budget_view'),
    
    # Media root for js libraries (d3, jquery, css, etc.)
    url(r'^media/(?P<path>.*)$', serve, {'document_root': settings.MEDIA_ROOT}),
    
    
    # Test Area
    url('test/$', views.testview, kwargs={'variable': 'there'}, name='test'),
    url('transfer/$', views.transfer_amount, name='transfer'),
    
    url(r'^login/$', auth_views.login, name = 'login'),
    url(r'^logout/$', auth_views.logout, name = 'logout'),

]