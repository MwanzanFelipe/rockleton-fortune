'''
Created on Aug 17, 2015

@author: edrogers
'''
from rockletonfortune.models import *
from django.contrib import admin

class TransactionAdmin(admin.ModelAdmin):
    pass
admin.site.register(Transaction, TransactionAdmin)

class Transaction_ImportAdmin(admin.ModelAdmin):
    pass
admin.site.register(Transaction_Import, Transaction_ImportAdmin)

class Primary_CategoryAdmin(admin.ModelAdmin):
    pass
admin.site.register(Primary_Category, Primary_CategoryAdmin)

class Primary_Category_BucketAdmin(admin.ModelAdmin):
    pass
admin.site.register(Primary_Category_Bucket, Primary_Category_BucketAdmin)

class Secondary_CategoryAdmin(admin.ModelAdmin):
    pass
admin.site.register(Secondary_Category, Secondary_CategoryAdmin)

class SourceAdmin(admin.ModelAdmin):
    pass
admin.site.register(Source, SourceAdmin)

class BudgetAdmin(admin.ModelAdmin):
    pass
admin.site.register(Budget, BudgetAdmin)

class Source_CategoryAdmin(admin.ModelAdmin):
    pass
admin.site.register(Source_Category, Source_CategoryAdmin)

class BckGrnd_ClcsAdmin(admin.ModelAdmin):
    pass
admin.site.register(BckGrnd_Clcs, BckGrnd_ClcsAdmin)

class RockletonAdmin(admin.ModelAdmin):
    pass
admin.site.register(Rockleton, RockletonAdmin)