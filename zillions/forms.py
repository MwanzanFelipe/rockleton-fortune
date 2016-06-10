from django import forms
from django.forms.models import inlineformset_factory, modelformset_factory, BaseInlineFormSet, formset_factory
from django.forms.formsets import DELETION_FIELD_NAME
from django.contrib.auth.models import User

from zillions.models import Transaction, Primary_Category_Bucket, Primary_Category, Secondary_Category, Source_Category, Source, Budget, Rockleton

class TransactionForm(forms.ModelForm):
    class Meta:
        model = Transaction
        fields = '__all__'
        #fields = ['name', 'description', 'notes', 'workout_date', 'workout_plan']


#Multiple budget lines per secondary transaction enabling one for ed and one for julie         
BudgetFormSet = inlineformset_factory(Secondary_Category, Budget, extra=1, can_delete=False, max_num=2, fields = '__all__')

class SelectFileForm(forms.Form):
    file = forms.FileField()

class UserForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput())
    class Meta:
        model = User  
        fields = ['username', 'email', 'password']

class RockletonForm(forms.ModelForm):
    class Meta:
        model = Rockleton
        exclude = ['user']
        
class TransferForm(forms.ModelForm):
    class Meta:
        model = Transaction
        fields = ['source','secondary_category']
        
class BaseTransferForm(forms.ModelForm):
    class Meta:
        model = Transaction
        fields = ['transaction_date','description','amount','ed_perc']
        

