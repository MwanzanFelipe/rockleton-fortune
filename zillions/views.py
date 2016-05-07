from django.http import HttpResponse
from django.template import RequestContext, loader, Library
from itertools import chain
from operator import attrgetter
from datetime import datetime, timedelta, date
from collections import OrderedDict as SortedDict
from django.contrib import messages
import copy
import csv
from decimal import *
from django.db.models import Sum, Count, F, Q
from django.shortcuts import render_to_response, get_object_or_404, redirect

from django.views.generic import CreateView, UpdateView

from rockletonfortune.models import Transaction, Primary_Category_Bucket, Primary_Category, Secondary_Category, Source_Category, Source, Budget, Transaction_Import

from .forms import *
from .z_queries import *

from django.forms.formsets import formset_factory
from django.forms.models import modelformset_factory, inlineformset_factory

from django.contrib.auth.models import User
#from django.contrib.auth.decorators import login_required, user_passes_test

def index(request):
    
    #Add budget allocations to each category on Sunday
    weekly_reup(request)

    # Primary Category Totals for Ed
    category_totals = q_ed_primary_buckets()

    # Show signed amounts along side Account names (source) and Source Categories
    source_totals = q_ed_sources()
    
    # Subtract Ed + Julie total from the account total. If = 0, then reconciled
    reconciliation = q_reconciliation()
    
    #Get latest transaction date
    last_updated = q_latest_transaction_date()
    
    #Get Julie Total
    julie_total = q_julie_total()
    
    #Flagged Count
    flagged = q_transaction_list(primary = None, 
                                       secondary = None,
                                       source = None,
                                       individual = request.user.first_name.lower(),
                                       startdate = None,
                                       enddate = None,
                                       description = '',
                                       amount_lte = None,
                                       amount_gte = None,
                                       transaction_type = None,
                                       incl_internal_transfer = None,
                                       flagged = 1)

    template = loader.get_template('index.html')
    context = RequestContext(request, {
        'category_totals' : category_totals,
        'source_totals' : source_totals,
        'reconciliation' : reconciliation,
        'last_updated' : last_updated,
        'julie_total' : julie_total,
        'flagged' : flagged
    })
    return HttpResponse(template.render(context))

def weekly_reup(request):
    # Get the last time the budget was applied
    bckgrnd_calcs = BckGrnd_Clcs.objects.get(pk=1)
    last_reup_week_number = bckgrnd_calcs.last_week_updated
    
    # Compare last reup to current week
    current_week_number = int(date.today().strftime("%U"))
    if last_reup_week_number == int(date(date.today().year - 1, 12, 31).strftime("%U")):
        #Compensate for weeks that cross over into next year
        last_reup_week_number = 0
        
    # Calc the number of times the reup must iterate for
    reup_delta = current_week_number - last_reup_week_number
    if reup_delta < 0:
        reup_delta = 0
    
    time_period = {
        "WE" : 1,
        "MO" : 4,
    }
    Scndry_Ctgry=Secondary_Category.objects.all()
    Srce=Source.objects.all()
    
    for i in range(0, reup_delta):
        weekly_budget_total = 0
        # Hard coded to update only Ed's budget
        for budget_item in Budget.objects.filter(user__user__first_name__icontains='ed'):
            
            transaction = Transaction()
            
            transaction.transaction_date = date.today()
            transaction.description = "Week %s - %s ReUp" % (last_reup_week_number + i + 1, budget_item.secondary_category.name)
            
            transaction.amount = budget_item.amount * (budget_item.ed_perc / 100) / time_period[budget_item.time_period]
            
            #Keep track of the total amount "disbursed"
            weekly_budget_total = weekly_budget_total + transaction.amount
            
            transaction.transaction_type = 1
            transaction.secondary_category_id = budget_item.secondary_category.id
            
            #Assign all disbursements to BofA Checking
            transaction.source_id = filter(lambda x: x.name == "BofA Checking", Srce)[0].id
            transaction.notes = "Weekly ReUp"
            transaction.ed_perc = 100
            transaction.mint_import = 0
            transaction.internal_transfer = 1

            transaction.save()
            
        #Offset disbursements with debit from Income
        transaction = Transaction()
        transaction.transaction_date = date.today()
        transaction.description = "Week %s - Weekly Transfer" % (last_reup_week_number + i + 1)
        transaction.amount = weekly_budget_total
        transaction.transaction_type = -1
        transaction.secondary_category_id = filter(lambda x: x.name == "Income", Scndry_Ctgry)[0].id 
        transaction.source_id = filter(lambda x: x.name == "BofA Checking", Srce)[0].id
        transaction.notes = "Weekly ReUp"
        transaction.ed_perc = 100
        transaction.mint_import = 0
        transaction.internal_transfer = 1
        transaction.save()
    
    #Record date of last reup
    if reup_delta > 0:
        bckgrnd_calcs.last_week_updated = current_week_number
        bckgrnd_calcs.date_updated = date.today()
        bckgrnd_calcs.save()
        success_msg = "Weekly ReUp Complete. %s week(s) credited." % reup_delta
        messages.success(request, success_msg)
    
    return

def import_transactions(request):

    if request.method == 'POST':
        form = SelectFileForm(request.POST, request.FILES)
        
        if form.is_valid():
            #Future: remove transaction_list and rename wip
            transaction_list, transaction_list_wip = handle_selected_file(request.FILES['file'])
            #Pass the list of possible duplicates to the next function via session
            request.session['transaction_list'] = transaction_list_wip
            
            template = loader.get_template('transaction_import.html')

            context = RequestContext(request, {
                'transaction_list' : transaction_list,
                'transaction_list_wip' : transaction_list_wip
            })

            return HttpResponse(template.render(context))
    else:
        form = SelectFileForm()
    template = loader.get_template('selectfile.html')
    context = RequestContext(request, {'form': form})
    return HttpResponse(template.render(context))


def handle_selected_file(f):
    # process the selected file for import into format to be compared to existing transactions
    
    i = 0
    
    Scndry_Ctgry=Secondary_Category.objects.all()
    Srce=Source.objects.all()
    
    # Clear the import buffer
    Transaction_Import.objects.all().delete()
    
    for line in f:
        # Skip the header row
        if i > 0:

            # Assumes file is comma-delimmeted
            line =  line.split('","')
            
            # initialize the model object
            transaction = Transaction_Import()
            
            # Determine sign of amount  
            DEBIT = -1
            CREDIT = 1
            if line[4]=="credit":
                line[4]=CREDIT
            else:
                line[4]=DEBIT
            
            # Reformat the transaction date to make sure the MM has a leading 0
            transaction_date = line[0][1:].split('/')
            if transaction_date[0] in ("1","2","3","4","5","6","7","8","9"):
                transaction_date[0] = str("0" + transaction_date[0])
            transaction_date =  transaction_date[2] + "-" + transaction_date[0] + "-" + transaction_date[1]

            # Only import transactions that occurred in the last month and a half
            # FUTURE: reduce the length of time
            last_updated = q_latest_transaction_date()
            if transaction_date > str(last_updated - timedelta(days=45)):
                                      
                transaction.transaction_date = transaction_date
                transaction.description = line[1]
                transaction.original_description = line[2]
                transaction.amount = Decimal(line[3])
                transaction.transaction_type = line[4]
                transaction.secondary_category = str(line[5])
                transaction.source = str(line[6])
            
                transaction.save()
        i+=1 
        

    # Get transactions from the import table that do not have an exact match to transactions from Mint
    perc_bound = Decimal(0.3)
    transaction_list, transaction_list_wip = q_new_transactions(perc_bound)

    # FUTURE, WIP is now the SSOT
    
    return transaction_list, transaction_list_wip
    


def import_transaction_input(request):
    # For each table that returned a value of 0, add to a list to pass to transaction_import_input.html
    # Also pass along the ti and id number of values >0

    # Get transactions from the import table that do not have an exact match to transactions from Mint
    # FUTURE, WIP is now the SSOT
    perc_bound = Decimal(0.3)
    transaction_list, transaction_list_wip = q_new_transactions(perc_bound)
    # Get the transaction/imported transaction comparison from the session
    transaction_list_wip = request.session['transaction_list']
    
    new_transactions = []
    new_transactions_wip = {}
    new_transactions_wip['new'] = []
    new_transactions_wip['dupe'] = []

    # FUTURE need to account for if there were no selections made
    # FUTURE, this for loop is deprecated because it relies on transaction_list, not WIP
    for transaction in transaction_list:
        post_string = "transaction_" + str(transaction[0]['id'])
        
        try:
            if request.POST[post_string][:2] == "ti":
                new_transactions.append(transaction)
            else:
                pass
        except:
            pass

    # Loop over imported transactions that have could be duplicates
    # Get the pairing of new transaction / replace transaction
    for transaction in transaction_list_wip['dupe']:
        post_string = "transaction_" + str(transaction[0]['id'])
        
        
        try:
            # The previous form labels the rows t_ or ti (an original transaction / an imported transaction)
            # If a ti_ is found, then the user chose to import an imported transaction as new
            if request.POST[post_string][:2] == "ti":
                new_transactions_wip['new'].append(transaction)
            else:
                # If a ti_ is not found, then the user chose to replace an existing transaction
                
                replaced_transaction = Transaction.objects.get(pk=request.POST[post_string][2:])

                # Group the imported transaction with the replaced transaction
                transaction_group = [transaction, replaced_transaction]
                new_transactions_wip['dupe'].append(transaction_group)
        except:
            pass

    # Loop over imported transactions that have no possibility of being duplicates
    for transaction in transaction_list_wip['new']:
        post_string = "transaction_" + str(transaction['id'])
        
        try:
            # The previous form labels the rows t_ or ti (an original transaction / an imported transaction)
            # If a ti_ is found, then the user chose to import an imported transaction as new
            if request.POST[post_string][:2] == "ti":
                # Adding one item to a list so the template iterates the dupe transactions the same way it does the new ones
                transaction_group = [transaction]
                new_transactions_wip['new'].append(transaction_group)
            else:
                pass
        except:
            pass

    # Save the final import transactions to session 
    request.session['new_transactions'] = new_transactions_wip
    
    template = loader.get_template('transaction_import_input.html')
    context = RequestContext(request, {
        'new_transactions' : new_transactions, 
        'new_transactions_wip' : new_transactions_wip
    })
    return HttpResponse(template.render(context))

def import_transaction_save(request):

    Scndry_Ctgry=Secondary_Category.objects.all()
    Srce=Source.objects.all()

    # FUTURE, pass perc_bound to each function instead of defining it in 3 places
    perc_bound = Decimal(0.3)
    transaction_list, transaction_list_wip = q_new_transactions(perc_bound)
    
    # Get the final import transactions to session 
    new_transactions_wip = request.session['new_transactions']
    
    # Save new transactions along with user detail for ed_perc, notes, alias
    for transaction in new_transactions_wip['new']:
        
        new_transaction = Transaction()
        try:

            post_edperc_string = "transaction_" + str(transaction[0]['id']) + "_edperc"
            post_notes_string = "transaction_" + str(transaction[0]['id']) + "_notes"
            post_alias_string = "transaction_" + str(transaction[0]['id']) + "_alias"
            post_flag_string = "transaction_" + str(transaction[0]['id']) + "_flag"

            if request.POST[post_edperc_string] == "":
                #if ed_perc left blank, do not import
                pass 
            else:
                new_transaction.ed_perc = Decimal(request.POST[post_edperc_string])
                
            

            new_transaction.transaction_date = transaction[0]['transaction_date']
            new_transaction.description = transaction[0]['description']
            new_transaction.original_description = transaction[0]['original_description']
            new_transaction.amount = transaction[0]['amount']
            new_transaction.transaction_type = transaction[0]['transaction_type']
            
            new_transaction.secondary_category_id = filter(lambda x: x.name == transaction[0]['secondary_category'], Scndry_Ctgry)[0].id
            new_transaction.source_id = filter(lambda x: x.name == transaction[0]['source'], Srce)[0].id

            new_transaction.notes = request.POST[post_notes_string]
            new_transaction.alias = request.POST[post_alias_string]
            
            #Future: There should be a default value for the flag coming through.
            try:
                new_transaction.flagged = request.POST[post_flag_string]
            except:
                new_transaction.flagged = 0
            
            new_transaction.mint_import = 1
            
            new_transaction.save()

        except:
            pass
        
    # Replace duplicate transactions with imported transactions
    for transaction in new_transactions_wip['dupe']:
        # need double array reference because this is replacing one with another

        transaction[1].transaction_date = transaction[0][0]['transaction_date']
        transaction[1].description = transaction[0][0]['description']
        transaction[1].original_description = transaction[0][0]['original_description']
        transaction[1].amount = transaction[0][0]['amount']
        transaction[1].transaction_type = transaction[0][0]['transaction_type']
    
        transaction[1].secondary_category_id = filter(lambda x: x.name == transaction[0][0]['secondary_category'], Scndry_Ctgry)[0].id
        transaction[1].source_id = filter(lambda x: x.name == transaction[0][0]['source'], Srce)[0].id

            
        transaction[1].save()
    # FUTURE, this needs to be changed to indicate the actual number of transactions imported
    # If ed_perc undefined, transaction isn't imported, but it is counted here
    success_msg = "%s new transaction(s) added. %s transaction(s) replaced." % (len(new_transactions_wip['new']),len(new_transactions_wip['dupe']) )
    messages.success(request, success_msg)
        
    return redirect('index')

def import_current_fortune():
    # File name
    f = open('/Users/edrogers/Documents/development/zillions/zillions/rockleton/ancillary.txt', 'r')
    
    # Variable for import row
    i = 0
    
    Scndry_Ctgry=Secondary_Category.objects.all()
    Srce=Source.objects.all()
    
    for line in f:
        # Skip the header row
        if i > 0:

            # Assumes file is comma-delimmeted
            line =  line.split('"",""')
            
            # initialize the model object
            transaction = Transaction()
            
            # Determine sign of amount  
            DEBIT = -1
            CREDIT = 1
            if line[4]=="credit":
                line[4]=CREDIT
            else:
                line[4]=DEBIT

            # If percent is 1 vs 0.5, change assumptions on the length of this string
            # This will not be relevant once we start importing directly from mint csv
            if line[8][1]== "\"":
                line[8]= line[8][0:1]
            else:
                line[8]= line[8][0:3]

            # Change assumption about length of date (12/1/15 vs 12/11/15)
            MM=line[0][3:5]
            YY=line[0][-2:]
            DD=line[0][6:8]
            if MM[1]=="/":
                MM="0"+MM[0]
                DD=line[0][5:7]
            if DD[1]=="/":
                DD="0"+DD[0]

            transaction.transaction_date = "20" + YY + "-" + MM + "-" + DD
            transaction.description = line[1]
            transaction.original_description = line[2]
            transaction.amount = Decimal(line[3])
            transaction.transaction_type = line[4]
            # FUTURE: Is there a better way to identify the secondary category/source? Filter seems inefficient?
            transaction.secondary_category_id = filter(lambda x: x.name == str(line[5]), Scndry_Ctgry)[0].id
            transaction.source_id = filter(lambda x: x.name == str(line[6]), Srce)[0].id
            transaction.notes = line[7]
            transaction.ed_perc = Decimal(line[8])
            
            transaction.save()
        i+=1        

    return


        


    

def transaction_list(request):
    
    #Get search parameters from request object
    try:
        source = request.GET['source']
    except:
        source = None
        
    try:
        secondary = request.GET['secondary']
    except:
        secondary = None

    try:
        primary = request.GET['primary']
    except:
        primary = None
        
    try:
        individual = request.GET['individual']
    except:
        individual = None
        
    try:
        amount_gte = request.GET['amount_gte']
    except:
        amount_gte = None
    try:
        amount_lte = request.GET['amount_lte']
    except:
        amount_lte = None
    try:
        transaction_type = request.GET['transaction_type']
    except:
        transaction_type = None

    try:
        startdate = request.GET['startdate']
    except:
        # Default to a date earlier than any possible transaction
        startdate = "2012-01-01"

    try:
        enddate = request.GET['enddate']
    except:
        enddate = date.today()
        
    try:
        description = request.GET['description']
    except:
        description = ''

    try:
        incl_internal_transfer = int(request.GET['incl_internal_transfer'])
    except:
        incl_internal_transfer = ''
        
    try:
        flagged = int(request.GET['flagged'])
    except:
        flagged = ''
    
    # Get spreadsheet view of transactions, including primary / secondary category name, source name, and week/year number
    transaction_list = q_transaction_list(primary, 
                                       secondary,
                                       source,
                                       individual,
                                       startdate,
                                       enddate,
                                       description,
                                       amount_lte,
                                       amount_gte,
                                       transaction_type,
                                       incl_internal_transfer,
                                       flagged)
    
    template = loader.get_template('transaction_list.html')
    context = RequestContext(request, {
        'transaction_list' : transaction_list
    })
    return HttpResponse(template.render(context))


class AddTransactionView(CreateView):
    template_name = 'edit_transaction.html'
    form_class = TransactionForm

    def get_context_data(self, **kwargs):
        context = super(AddTransactionView, self).get_context_data(**kwargs)
        return context

    def form_valid(self, form):
        context = self.get_context_data()
        self.object = form.save()
    
        #return redirect(self.object.get_absolute_url())  # assuming your model has ``get_absolute_url`` defined.
        return redirect('index')
    

class UpdateTransactionView(UpdateView):

    model = Transaction
    template_name = 'edit_transaction.html'
    fields = '__all__'

    success_url = '/zillions/'


def export_csv(request):

    source = None
    secondary = None
    primary = None
    description = ''
    amount_lte = None
    amount_gte = None
    transaction_type = None
    individual = 'julie'
    startdate = "2012-01-01"
    enddate = date.today()
    
    individual = "julie"
    
    # Get spreadsheet view of transactions, including primary / secondary category name, source name, and week/year number
    transaction_list = q_transaction_list(primary, 
                                       secondary,
                                       source,
                                       individual,
                                       startdate,
                                       enddate,
                                       description,
                                       amount_lte,
                                       amount_gte,
                                       transaction_type,
                                       incl_internal_transfer = 1,
                                       flagged = 0)
    #****************    
    
    # Primary Category Totals for Ed
    category_totals = q_ed_primary_buckets()

    # Show signed amounts along side Account names (source) and Source Categories
    source_totals = q_ed_sources()

    #Get Julie Amortization Total
    julie_amort_total = q_julie_amortization_total()
    
    #Get Julie Total
    julie_total = q_julie_total()
    
    # **********
    
    # Create the HttpResponse object with the appropriate CSV header.
    response = HttpResponse(content_type='text/csv')
    csv_str = "attachment; filename=julie_export_%s.csv" % (str(date.today()))
    response['Content-Disposition'] = csv_str
    
    writer = csv.writer(response)
    writer.writerow(['Date', 'Description', 'Amount', 'Primary Category', 'Primary Category Categories', 'Account Name', 'Ed %', 'Julie %', 'Julie $', 'Week Number', '', '', 'Account', 'Balance', 'Remaining'])
    writer.writerow([transaction_list[0]['transaction_date'], transaction_list[0]['description'], -1 * transaction_list[0]['signed_amount'], transaction_list[0]['primary_category'], transaction_list[0]['primary_category_category'], transaction_list[0]['source_name'], transaction_list[0]['ed_perc']/100, transaction_list[0]['julie_perc']/100, -1 * transaction_list[0]['julie_signed_amount'], str(transaction_list[0]['year_number']) + ' - Week Number ' + str(transaction_list[0]['week_number']), '', '', 'BofA CC', source_totals['Credit Card']['BofA CC'], 7000 + source_totals['Credit Card']['BofA CC'] ])
    writer.writerow([transaction_list[1]['transaction_date'], transaction_list[1]['description'], -1 * transaction_list[1]['signed_amount'], transaction_list[1]['primary_category'], transaction_list[1]['primary_category_category'], transaction_list[1]['source_name'], transaction_list[1]['ed_perc']/100, transaction_list[1]['julie_perc']/100, -1 * transaction_list[1]['julie_signed_amount'], str(transaction_list[1]['year_number']) + ' - Week Number ' + str(transaction_list[1]['week_number']), '', '', 'Citi', source_totals['Credit Card']['Citi'], 13000 + source_totals['Credit Card']['Citi'] ])
    writer.writerow([transaction_list[2]['transaction_date'], transaction_list[2]['description'], -1 * transaction_list[2]['signed_amount'], transaction_list[2]['primary_category'], transaction_list[2]['primary_category_category'], transaction_list[2]['source_name'], transaction_list[2]['ed_perc']/100, transaction_list[2]['julie_perc']/100, -1 *  transaction_list[2]['julie_signed_amount'], str(transaction_list[2]['year_number']) + ' - Week Number ' + str(transaction_list[2]['week_number']), '', '', '', '', ''])
    writer.writerow([transaction_list[3]['transaction_date'], transaction_list[3]['description'], -1 * transaction_list[3]['signed_amount'], transaction_list[3]['primary_category'], transaction_list[3]['primary_category_category'], transaction_list[3]['source_name'], transaction_list[3]['ed_perc']/100, transaction_list[3]['julie_perc']/100, -1 *  transaction_list[3]['julie_signed_amount'], str(transaction_list[3]['year_number']) + ' - Week Number ' + str(transaction_list[3]['week_number']), '', '', 'BofA', source_totals['Account']['BofA Checking'], '' ])
    writer.writerow([transaction_list[4]['transaction_date'], transaction_list[4]['description'], -1 * transaction_list[4]['signed_amount'], transaction_list[4]['primary_category'], transaction_list[4]['primary_category_category'], transaction_list[4]['source_name'], transaction_list[4]['ed_perc']/100, transaction_list[4]['julie_perc']/100, -1 *  transaction_list[4]['julie_signed_amount'], str(transaction_list[4]['year_number']) + ' - Week Number ' + str(transaction_list[4]['week_number']), '', '', 'Amortized', -category_totals['Debt']['Amortize'], '' ])
    writer.writerow([transaction_list[5]['transaction_date'], transaction_list[5]['description'], -1 * transaction_list[5]['signed_amount'], transaction_list[5]['primary_category'], transaction_list[5]['primary_category_category'], transaction_list[5]['source_name'], transaction_list[5]['ed_perc']/100, transaction_list[5]['julie_perc']/100, -1 *  transaction_list[5]['julie_signed_amount'], str(transaction_list[5]['year_number']) + ' - Week Number ' + str(transaction_list[5]['week_number']), '', '', '', '', ''])
    writer.writerow([transaction_list[6]['transaction_date'], transaction_list[6]['description'], -1 * transaction_list[6]['signed_amount'], transaction_list[6]['primary_category'], transaction_list[6]['primary_category_category'], transaction_list[6]['source_name'], transaction_list[6]['ed_perc']/100, transaction_list[6]['julie_perc']/100, -1 *  transaction_list[6]['julie_signed_amount'], str(transaction_list[6]['year_number']) + ' - Week Number ' + str(transaction_list[6]['week_number']), '', '', 'Wedding Fund', category_totals['Special']['Wedding Fund'], category_totals['Special']['Wedding Fund']])
    writer.writerow([transaction_list[7]['transaction_date'], transaction_list[7]['description'], -1 * transaction_list[7]['signed_amount'], transaction_list[7]['primary_category'], transaction_list[7]['primary_category_category'], transaction_list[7]['source_name'], transaction_list[7]['ed_perc']/100, transaction_list[7]['julie_perc']/100, -1 *  transaction_list[7]['julie_signed_amount'], str(transaction_list[7]['year_number']) + ' - Week Number ' + str(transaction_list[7]['week_number']), '', '', 'Furniture', category_totals['Special']['Furniture'], category_totals['Special']['Furniture']])
    writer.writerow([transaction_list[8]['transaction_date'], transaction_list[8]['description'], -1 * transaction_list[8]['signed_amount'], transaction_list[8]['primary_category'], transaction_list[8]['primary_category_category'], transaction_list[8]['source_name'], transaction_list[8]['ed_perc']/100, transaction_list[8]['julie_perc']/100, -1 *  transaction_list[8]['julie_signed_amount'], str(transaction_list[8]['year_number']) + ' - Week Number ' + str(transaction_list[8]['week_number']), '', '', 'Moving Account', category_totals['Special']['Moving Account'], category_totals['Special']['Moving Account']])
    writer.writerow([transaction_list[9]['transaction_date'], transaction_list[9]['description'], -1 * transaction_list[9]['signed_amount'], transaction_list[9]['primary_category'], transaction_list[9]['primary_category_category'], transaction_list[9]['source_name'], transaction_list[9]['ed_perc']/100, transaction_list[9]['julie_perc']/100, -1 *  transaction_list[9]['julie_signed_amount'], str(transaction_list[9]['year_number']) + ' - Week Number ' + str(transaction_list[9]['week_number']), '', '', '', '', ''])
    writer.writerow([transaction_list[10]['transaction_date'], transaction_list[10]['description'], -1 * transaction_list[10]['signed_amount'], transaction_list[10]['primary_category'], transaction_list[10]['primary_category_category'], transaction_list[10]['source_name'], transaction_list[10]['ed_perc']/100, transaction_list[10]['julie_perc']/100, -1 *  transaction_list[10]['julie_signed_amount'], str(transaction_list[10]['year_number']) + ' - Week Number ' + str(transaction_list[10]['week_number']), '', '', '', '', ''])
    writer.writerow([transaction_list[11]['transaction_date'], transaction_list[11]['description'], -1 * transaction_list[11]['signed_amount'], transaction_list[11]['primary_category'], transaction_list[11]['primary_category_category'], transaction_list[11]['source_name'], transaction_list[11]['ed_perc']/100, transaction_list[11]['julie_perc']/100, -1 *  transaction_list[11]['julie_signed_amount'], str(transaction_list[11]['year_number']) + ' - Week Number ' + str(transaction_list[11]['week_number']), '', '', 'Last Updated', transaction_list[0]['transaction_date'], ''])
    writer.writerow([transaction_list[12]['transaction_date'], transaction_list[12]['description'], -1 * transaction_list[12]['signed_amount'], transaction_list[12]['primary_category'], transaction_list[12]['primary_category_category'], transaction_list[12]['source_name'], transaction_list[12]['ed_perc']/100, transaction_list[12]['julie_perc']/100, -1 *  transaction_list[12]['julie_signed_amount'], str(transaction_list[12]['year_number']) + ' - Week Number ' + str(transaction_list[12]['week_number']), '', '', '', '', ''])
    writer.writerow([transaction_list[13]['transaction_date'], transaction_list[13]['description'], -1 * transaction_list[13]['signed_amount'], transaction_list[13]['primary_category'], transaction_list[13]['primary_category_category'], transaction_list[13]['source_name'], transaction_list[13]['ed_perc']/100, transaction_list[13]['julie_perc']/100, -1 *  transaction_list[13]['julie_signed_amount'], str(transaction_list[13]['year_number']) + ' - Week Number ' + str(transaction_list[13]['week_number']), '', '', 'Owed', julie_total, ''])
    writer.writerow([transaction_list[14]['transaction_date'], transaction_list[14]['description'], -1 * transaction_list[14]['signed_amount'], transaction_list[14]['primary_category'], transaction_list[14]['primary_category_category'], transaction_list[14]['source_name'], transaction_list[14]['ed_perc']/100, transaction_list[14]['julie_perc']/100, -1 *  transaction_list[14]['julie_signed_amount'], str(transaction_list[14]['year_number']) + ' - Week Number ' + str(transaction_list[14]['week_number']), '', '', 'Amortized', julie_amort_total, ''])
    for i in range(15, len(transaction_list)):
        writer.writerow([transaction_list[i]['transaction_date'], transaction_list[i]['description'], -1 * transaction_list[i]['signed_amount'], transaction_list[i]['primary_category'], transaction_list[i]['primary_category_category'], transaction_list[i]['source_name'], transaction_list[i]['ed_perc']/100, transaction_list[i]['julie_perc']/100, -1 *  transaction_list[i]['julie_signed_amount'], str(transaction_list[i]['year_number']) + ' - Week Number ' + str(transaction_list[i]['week_number']), '', '', '', '', ''])

    return response


def create_budget_universe(post, files):
    #By budget universe, we mean budget items across all secondary categories
    
    #BudgetFormSet = inlineformset_factory(Secondary_Category, Budget, extra=1, can_delete=False, max_num=2)
    budget_universe = []

    # group budgets by pc
    for pc in Primary_Category.objects.all():
        primary_category = {}
        primary_category['name'] = pc.name
        primary_category['secondary_categories'] = []
        
        # for each sc, create a formset using either data that exists (post) or blank
        for sc in pc.secondary_category_set.all():
            #secondary category is a dictionary of the sc.name and the sc.formset 
            secondary_category = {}
            secondary_category['name'] = sc.name
            if post is not None:
                formset = BudgetFormSet(post, files, instance = sc, prefix = 'BUDGETS_%s' % sc.id)
                
            else:  
                formset = BudgetFormSet(instance = sc, prefix = 'BUDGETS_%s' % sc.id)
                #bfs = inlineformset_factory(Rockleton, Budget, extra=1, can_delete=False)
                #ed= Rockleton.objects.filter(user__first_name__icontains='ed')[0]
                #print bfs(instance = ed, prefix = 'BUDGETS_%s' % sc.id)


            #Populate formset for every secondary category
            secondary_category['formset'] = [] 
            secondary_category['formset'].append(formset)
            #Organize secondary categories by primary category
            primary_category['secondary_categories'].append(secondary_category)
        budget_universe.append(primary_category)
    return budget_universe

        
def get_or_none(model, *args, **kwargs):
    try:
        return model.objects.get(*args, **kwargs)
    except model.DoesNotExist:
        return None

def budget_edit(request):

    if request.method == 'POST':

        budget_universe = create_budget_universe(request.POST, request.FILES)
        valid = True
        for pc in budget_universe:
            for sc in pc['secondary_categories']:
                formset = sc['formset'][0]

                if not formset.is_valid():
                    valid = False
                    #Only continue if every secondary category entry is valid
                    
        if valid:

            #print request.user.first_name
            
            for pc in budget_universe:
                for sc in pc['secondary_categories']:
                    formset = sc['formset'][0]
                    instances = formset.save()
                    
            for sc in Secondary_Category.objects.all():
                #Get ed and julie user objects
                ed = get_or_none(Rockleton, user__first_name__iexact='ed')
                julie = get_or_none(Rockleton, user__first_name__iexact='julie')
                
                #Get budget per ed 
                ed_perc = get_or_none(Budget, secondary_category = sc.id, user = ed.id)

                #If ed budget left blacnk, set to 0
                if ed_perc is None:
                    ed_perc = 0
                else:
                    ed_perc = ed_perc.ed_perc
                    
                    #Get budget per julie
                julie_perc = get_or_none(Budget, secondary_category = sc.id, user = julie.id)
                
                #If julie budget left blacnk, set to 0
                if julie_perc is None:
                    julie_perc = 0
                else:
                    julie_perc = 100 - julie_perc.ed_perc

                # If ed_perc + julie_perc <>0 or 100 then flag
                # FUTURE: flag to alert, not notification
                if ed_perc + julie_perc != 0 and ed_perc + julie_perc != 100:
                    warning_msg = "Budget for Secondary Category %s does not equal 100%% between Ed and Julie." % (sc)
                    messages.warning(request, warning_msg)
            return redirect('index')

    else:
        budget_universe = create_budget_universe(None, None)

    template = loader.get_template('budget_edit.html')
    context = RequestContext(request, {'budget_universe': budget_universe})
    return HttpResponse(template.render(context))


def budget_view(request):
    
    try:
        startdate = datetime.strptime(request.GET['startdate'], '%Y%m%d').date()
    except:
        # Default to 6 weeks earlier
        startdate = date.today() - timedelta(days=42)
        
    try:
        enddate = datetime.strptime(request.GET['enddate'], '%Y%m%d').date()
    except:
        # Default to today
        enddate = date.today()
        
    #Determine if budget is reporting on the current week
    #If so, template will show current total surplus and the multiples of the budget that the surplus represents
    current_week_flag = 0
    if enddate == date.today():
        current_week_flag = 1
    
    #Number of weeks over which to calculate the moving average
    try:
        moving_avg_weeks = int(request.GET['moving_avg_weeks'])
    except:
        moving_avg_weeks = 6
        
    try:
        individual = request.GET['individual']
    except:
        individual = None

    # Calculate start and end dates as of the beginning and end of weeks
    startdate = startdate - timedelta(days=(startdate.weekday() + 1) % 7)
    enddate = enddate - timedelta(days=(enddate.weekday() + 1) % 7) + timedelta(days=6)
    
    # Determine number of weeks to show summary for
    delta = enddate - startdate
    num_weeks = (delta.days + 1) / 7
    
    template = loader.get_template('budget_view.html')
    context = RequestContext(request, {
        'startdate' : startdate,
        'enddate' : enddate,
        'individual' : individual,
        'current_week_flag' : current_week_flag,
        'num_weeks' : num_weeks,
        'moving_avg_weeks' : moving_avg_weeks
    })
    return HttpResponse(template.render(context))

# Test Area

def testview(request, variable="here"):
    test = variable
    template = loader.get_template('test_template.html')
    context = RequestContext(request, {
        'test': test
    })
    return HttpResponse(template.render(context))

def transfer_amount(request):
    form = BaseTransferForm
    TransferFormSet = formset_factory(TransferForm, extra = 2)
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        formset = TransferFormSet(request.POST)
        form = BaseTransferForm(request.POST)
        # check whether it's valid:
        if formset.is_valid() and form.is_valid():
            
            print request.POST
            i=0
            for formclip in formset:
                new_transaction = Transaction()
                new_transaction.ed_perc = Decimal(request.POST['ed_perc'])
                new_transaction.transaction_date = request.POST['transaction_date']
                new_transaction.description = request.POST['description']
                new_transaction.amount = request.POST['amount']
                new_transaction.internal_transfer = 1
                
                if i==0:
                    new_transaction.transaction_type = 1
                else: 
                    new_transaction.transaction_type = -1
            
                sc_string = 'form-' + str(i) + '-secondary_category'
                src_string = 'form-' + str(i) + '-source'
                new_transaction.secondary_category_id = request.POST[sc_string]
                new_transaction.source_id = request.POST[src_string]

            
                new_transaction.mint_import = 0
            
                new_transaction.save()
                
                i=1
          


            return redirect('index')

    # if a GET (or any other method) we'll create a blank form
    else:
        #form = TransferFormSet()
        pass



    template = loader.get_template('transfer.html')
    context = RequestContext(request, {'formset': TransferFormSet(), 'form':BaseTransferForm})
    return HttpResponse(template.render(context))

