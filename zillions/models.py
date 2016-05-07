from __future__ import unicode_literals

from django.db import models
from datetime import datetime
from django.contrib.auth.models import User

# Assign a sign based on how mint describes the transaction
DEBIT = -1
CREDIT = 1
TRANSACTION_TYPE_CHOICES = (
    (DEBIT, 'debit'),
    (CREDIT, 'credit'),
)

WEEK = 'WE'
MONTH = 'MO'
TIME_PERIOD_CHOICES = (
    (WEEK, 'Weekly'),
    (MONTH, 'Monthly'),
)

class Rockleton(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    date_of_birth = models.DateField()
    
    #julie/marina
    #julie/kenmare2014
    
    def __str__(self):
        return "User %s data" % self.user

# Each Primary Category falls into a cateogry type (Budgeted, Special, Hidden, etc)
class Primary_Category_Bucket(models.Model):
    name = models.CharField("Category Name", max_length=15)
    
    def __unicode__(self):
        return self.name
    
# Primary Categories are things like Food & Dining whereas Secondary categories like Groceries and Fast Food are beneath
class Primary_Category(models.Model):
    name = models.CharField("Category Name", max_length=30) 
    category = models.ForeignKey(Primary_Category_Bucket) 
    
    def __unicode__(self):
        return self.name

# Primary Categories are things like Food & Dining whereas Secondary categories like Groceries and Fast Food are beneath
class Secondary_Category(models.Model):
    name = models.CharField("Category Name", max_length=30)
    primary_category = models.ForeignKey(Primary_Category)
    
    def __unicode__(self):
        return self.name

# Account Types (Bank of America, Chase Credit Card) have categories (Checking, Credit Card, etc)    
class Source_Category(models.Model):
    name = models.CharField("Category Name", max_length=15)
    
    def __unicode__(self):
        return self.name
    
# Source is an account (Bank of America, Chase credit card, etc)
class Source(models.Model):
    name = models.CharField("Source Name", max_length=20)
    category = models.ForeignKey(Source_Category)
    
    def __unicode__(self):
        return self.name

# Weekly budget amounts are assigned to each secondary category
class Budget(models.Model):
    user = models.ForeignKey(Rockleton)
    amount = models.DecimalField(max_digits=7, decimal_places=2)
    secondary_category = models.ForeignKey(Secondary_Category)
    ed_perc = models.DecimalField(max_digits=5, decimal_places=2)
    time_period = models.CharField(max_length=2, choices=TIME_PERIOD_CHOICES,default=WEEK)
    
    def __unicode__(self):
         return u'%s Budget' % (self.secondary_category)
     
    class Meta:
         unique_together = ('user', 'secondary_category',)

# There is one transaction for every amount spent / earned / transferred
class Transaction(models.Model):
    transaction_date = models.DateField("Transaction Date")
    description = models.CharField("Description", max_length=60)
    # Mint attaches an electronic description to transactions. May or may not be the same as the Description
    original_description = models.CharField("Original Description", max_length=170, blank = True, null = True)
    # 4 decimal places because the budget sometimes creates weird multiplicative effects
    amount = models.DecimalField(max_digits=9, decimal_places=4)
    transaction_type = models.IntegerField(choices=TRANSACTION_TYPE_CHOICES)
    secondary_category = models.ForeignKey(Secondary_Category)
    source = models.ForeignKey(Source)
    ed_perc = models.DecimalField(max_digits=5, decimal_places=2)
    # Any notes I want to make about the transaction
    notes = models.TextField("Notes", blank=True)
    # Alias is the annotation the user attaches to a description. I need description and original description to match what mint has. But I can use alias to further describe a transaction if necessary
    alias = models.CharField("Alias", max_length=60, blank=True)
    mint_import = models.BooleanField(default=True)
    internal_transfer = models.BooleanField(default=False)
    flagged = models.BooleanField(default=False)

    # Use alias as the name if there is one
    def __unicode__(self):
        if not self.alias:
            return str(self.transaction_date) + " - " + self.description
        else:
            return str(self.transaction_date) + " - " + self.alias

    class Meta:
        ordering = ('-transaction_date',)

class Transaction_Import(models.Model):
    transaction_date = models.DateField("Transaction Date")
    description = models.CharField("Description", max_length=60)
    # Mint attaches an electronic description to transactions. May or may not be the same as the Description
    original_description = models.CharField("Original Description", max_length=170, blank = True, null = True)
    # 4 decimal places because the budget sometimes creates weird multiplicative effects
    amount = models.DecimalField(max_digits=9, decimal_places=4)
    transaction_type = models.IntegerField(choices=TRANSACTION_TYPE_CHOICES)
    secondary_category = models.CharField("Secondary Category", max_length=60)
    source = models.CharField("Source", max_length=60)

    # Use alias as the name if there is one
    def __unicode__(self):
        return str(self.transaction_date) + " - " + self.description

    class Meta:
        ordering = ('-transaction_date',)
        
class BckGrnd_Clcs(models.Model):
    last_week_updated = models.IntegerField()   
    date_updated = models.DateField("Date of ReUp")
    
