from django.db import connection
from collections import namedtuple
from decimal import *
from datetime import datetime, timedelta, date
from django.db.models import Sum, Count, F, Q
from django.core.serializers.json import DjangoJSONEncoder
import json
from django.http import HttpResponse
from collections import OrderedDict as SortedDict
#from django.contrib.auth.decorators import login_required

from zillions.models import *

def namedtuplefetchall(cursor):
    "Return all rows from a cursor as a namedtuple"
    desc = cursor.description
    nt_result = namedtuple('Result', [col[0] for col in desc])
    return [nt_result(*row) for row in cursor.fetchall()]

def cursor_execute(query_string):
    # Execute SQL query
    cursor = connection.cursor()
    cursor.execute(query_string)
    
    return cursor

def execute_query(query_string):
    #Convert SQL query to iterable object
    sql_tuple = namedtuplefetchall(cursor_execute(query_string))
    
    return sql_tuple

def split_list(list, filter):
    #If more than one keyword sent per url parameter, split them by comma
    split_list = list.split(',')
    quoted_split_list = ",".join('"' + item + '"' for item in split_list) 
    filter_string = " WHERE %s IN (%s) " % (filter, quoted_split_list)
    return filter_string

def drop_temp_table(table_name):
    query_string = "DROP TEMPORARY TABLE %s" % table_name
    try:
        cursor_execute(query_string)
    except:
        pass
    return

def pcb(bucket_name = None):
    #Get Primary Category Buckets
    
    if bucket_name is not None:
        bucket_name = split_list(bucket_name,'name')
    else:
        bucket_name = ''
        
    drop_temp_table('pcb')

    query_string = " \
        CREATE TEMPORARY TABLE pcb as ( \
            SELECT \
                * \
            FROM \
                zillions_primary_category_bucket \
            %s \
            );" % bucket_name

    return cursor_execute(query_string)

def pc_pcb(bucket_name, primary_names):
    #Get Primary Categories linked to Primary Category Buckets
    
    # Chain:
    # def pcb(bucket_name = None):
    
    if primary_names:
        primary_names = split_list(primary_names,'pc.name')
    else:
        primary_names = ''
    
    pcb(bucket_name)
    
    drop_temp_table('pc_pcb')
    
    query_string = " \
        CREATE TEMPORARY TABLE pc_pcb as ( \
            SELECT \
                pc.id as primary_category_id \
                , pc.name as primary_category \
                , pcb.name as primary_category_bucket \
            FROM \
                zillions_primary_category pc \
            INNER JOIN \
                pcb \
            ON \
                pc.category_id = pcb.id \
            %s \
            );" % primary_names

    return cursor_execute(query_string)

def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    # Get Secondary Categories
    # Linked to Primary Categories linked to Primary Category Buckets
    
    # Chain:
    # def pc_pcb(bucket_name, primary_names)
    #     def pcb(bucket_name = None):
    
    if secondary_names:
        secondary_names = split_list(secondary_names,'sc.name')
    else:
        secondary_names = ''
    
    pc_pcb(bucket_name, primary_names)
    
    drop_temp_table('sc_pc_pcb')
    
    query_string = " \
        CREATE TEMPORARY TABLE sc_pc_pcb as ( \
            SELECT \
                sc.id as secondary_category_id \
                , sc.name as secondary_category \
                , pc_pcb.primary_category \
                , pc_pcb.primary_category_id \
                , pc_pcb.primary_category_bucket \
            FROM \
                zillions_secondary_category sc \
            INNER JOIN \
                pc_pcb \
            ON \
                sc.primary_category_id = pc_pcb.primary_category_id \
            %s \
            );" % secondary_names

    return cursor_execute(query_string)

def s_sc():
    #Get Source category names for sources (along with sources)
    
    drop_temp_table('s_sc')
    
    query_string = " \
        CREATE TEMPORARY TABLE s_sc as ( \
            SELECT \
                s.id as source_id \
                , s.name as source \
                , sc.name as source_category \
            FROM \
                zillions_source s \
            LEFT JOIN \
                zillions_source_category sc \
             ON \
                s.category_id = sc.id \
            );"

    return cursor_execute(query_string)

def t_sc():
    #Get Secondary Category names for transactions
    
    #FUTURE: Does this join make sense? get all transactions??
    # Perhaps because this is used to find duplicate transactions that are imported
    # Though we could (should?) join after duplicates are found. This will then be a smaller join
    
    drop_temp_table('t_sc')
    
    query_string = " \
        CREATE TEMPORARY TABLE t_sc as ( \
            SELECT \
                t.id as transaction_id \
                , t.transaction_date \
                , t.description \
                , t.original_description \
                , t.amount \
                , t.transaction_type \
                , sc.name as secondary_category \
                , t.source_id \
            FROM  \
                zillions_transaction t \
            INNER JOIN \
                zillions_secondary_category sc \
            ON \
                t.secondary_category_id = sc.id \
            );"

    return cursor_execute(query_string)



def s_t_sc():
    #Get Source names for transactions/secondary category names
    
    # Chain:
    # def t_sc()
    
    t_sc()
    
    drop_temp_table('s_t_sc')
    
    query_string = " \
        CREATE TEMPORARY TABLE s_t_sc as ( \
            SELECT \
                t_sc.transaction_id \
                , t_sc.transaction_date \
                , t_sc.description \
                , t_sc.original_description \
                , t_sc.amount \
                , t_sc.transaction_type \
                , t_sc.secondary_category \
                , s.name as source \
            FROM \
                zillions_source s \
            INNER JOIN \
                t_sc \
            ON \
                s.id = t_sc.source_id \
            );"

    return cursor_execute(query_string)


def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None):
    #Get List of transactions given filter criteria
    # Linked to Secondary/Primary Categories linked to Primary Category Buckets
    
    # Chain:
    # def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #     def pc_pcb(bucket_name, primary_names)
    #         def pcb(bucket_name = None):
    
    #Initialize filters
    if startdate is None:
        startdate = "2012-01-01"
        
    if enddate is None:
        enddate = date.today()

    if amount_lte:
        amount_lte = "AND amount <= %s" % amount_lte
    else:
        amount_lte = ''
        
    if amount_gte:
        amount_gte = "AND amount >= %s" % amount_gte
    else:
        amount_gte = ''
        
    if transaction_type:
        transaction_type = "AND transaction_type = %s" % transaction_type
    else:
        transaction_type = ''

    if incl_internal_transfer == 0:
        incl_internal_transfer = "AND internal_transfer = 0"
    else:
        incl_internal_transfer = ''

    if flagged == 1:
        flagged = "AND flagged = 1"
    else:
       flagged = ''
        
    
    # By default, filter for perc > -1, therefore see everything
    ed_perc = -100
    julie_perc = -100
    
    if individual:
        if individual == "ed":
            # Filter out instances when ed is not involved at all (WHERE ed_perc > 0)
            ed_perc = 0
        elif individual == "julie":
            # Filter out instances when julie is not involved at all (WHERE (1-ed_perc) > 0)
            julie_perc = 0  
            
    # Apply description filter to original description and alias
    o_description = description
    alias = description
    
    sc_pc_pcb(bucket_name, primary_names, secondary_names)
    
    drop_temp_table('t_sc_pc_pcb')
    
    #Using logic on Year Number to account for times when year rolls over mid week
    query_string = " \
        CREATE TEMPORARY TABLE t_sc_pc_pcb as ( \
            SELECT \
                t.id as transaction_id \
                , t.transaction_date \
                , CASE \
                    WHEN t.alias = '' then t.description \
                    ELSE t.alias \
                    END as description \
                , t.transaction_type * t.amount as signed_amount \
                , sc_pc_pcb.secondary_category \
                , sc_pc_pcb.secondary_category_id \
                , sc_pc_pcb.primary_category \
                , sc_pc_pcb.primary_category_id \
                , sc_pc_pcb.primary_category_bucket \
                , t.source_id \
                , t.ed_perc as ed_perc \
                , (1-t.ed_perc/100) * 100 as julie_perc \
                , t.flagged \
                , t.transaction_type * t.amount * ed_perc/100 as ed_signed_amount \
                , t.transaction_type * t.amount * (1-ed_perc/100) as julie_signed_amount \
                , CASE \
                    WHEN WEEK(t.transaction_date,2) > WEEK(t.transaction_date) then YEAR(t.transaction_date) - 1 \
                    ELSE YEAR(t.transaction_date) \
                    END as year_number \
                , WEEK(t.transaction_date,2) as week_number \
            FROM \
                (SELECT \
                    * \
                FROM \
                    zillions_transaction \
                WHERE \
                    transaction_date >= '%s' \
                    AND transaction_date <= '%s' \
                    AND (description LIKE '%%%%%s%%%%' OR original_description LIKE '%%%%%s%%%%' OR alias LIKE '%%%%%s%%%%') \
                    %s %s %s %s %s\
                    AND ed_perc/100 > %s \
                    AND (1-ed_perc/100) > %s) t \
            INNER JOIN \
                sc_pc_pcb \
            ON t.secondary_category_id = sc_pc_pcb.secondary_category_id \
        );" % (startdate, enddate, description, o_description, alias, amount_lte, amount_gte, transaction_type, incl_internal_transfer, flagged, ed_perc, julie_perc)    

    return cursor_execute(query_string)

def dw_sc_pc_pcb(startdate, enddate):
    # Create a dummy table with every secondary category and every week in scope
    # This is used to populate 0 for weeks with no transactions (instead of simply omitting the week)
    # If we don't do this, a 3-week period with only one transaction will have an average equaling that one transaction
    # instead of averaging 2 0's and that transaction
    
    # sc_pc_pcb assumed to already exist
    
    drop_temp_table('dw_sc_pc_pcb')
    query_string = " \
        CREATE TEMPORARY TABLE dw_sc_pc_pcb as ( \
            SELECT \
                * \
            FROM \
                (SELECT \
                    CASE \
                        WHEN WEEK(a.Date,2) > WEEK(a.Date) then YEAR(a.Date) - 1 \
                        ELSE YEAR(a.Date) \
                        END as year_number \
                    , WEEK(a.Date,2) as week_number \
                FROM \
                    (SELECT \
                        curdate() - INTERVAL (a.a + (10 * b.a) + (100 * c.a)) DAY as Date \
                    FROM \
                        (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a \
                    CROSS JOIN \
                        (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b \
                    CROSS JOIN \
                        (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c) a \
                WHERE \
                    a.Date BETWEEN '%s' \
                    AND '%s' \
                GROUP BY 1,2) as dw, sc_pc_pcb); \
        " % (startdate, enddate)
    
    return cursor_execute(query_string)

def s(sources):
    # Get list of sources
    
    if sources: 
        sources = split_list(sources,'name')
    
    drop_temp_table('s')
    
    query_string = " \
        CREATE TEMPORARY TABLE s as ( \
            SELECT \
                * \
            FROM \
                zillions_source \
            %s \
        );" % (sources)

    return cursor_execute(query_string)

def q_ed_primary_buckets():

    # I would prefer to make queries using django's terminology
    # But I need to do some data transformations before I aggregate
    # I need to multiply transaction_type (-1,1) by amount before I sum by category
    # I can't find a way to do this
    # The below code is as close as I got
    # I included a @property in the model to get the signed_amount, but there's no way to annotate by a property
    # So for now, I'm going to write Raw SQL
    
    # Get Primary Category Totals for Ed
    # Show signed amounts multiplied by Ed Percent along side Primary Category Bucket and Primary Category Names
    # Collection Secondary Category ids to join to transactions
    # Collect Primary Category Names
    # Collect Primary Category buckets
    bucket_name = None
    primary_names = None
    secondary_names = None
    
    # Chain:
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None):


    sc_pc_pcb(bucket_name, primary_names, secondary_names)
    
    # Get sum of ed signed amount per primary category
    # FUTURE Generalize this to allow for Julie? Would only be relevant if she has budgets to replenish the primary categories
    query_string = " \
        SELECT \
            sc_pc_pcb.primary_category \
            , sc_pc_pcb.primary_category_bucket \
            , round(sum(t.transaction_type * t.amount * t.ed_perc/100),2) as ed_signed_amount \
        FROM \
            zillions_transaction t \
        LEFT JOIN \
            sc_pc_pcb \
        ON \
            t.secondary_category_id = sc_pc_pcb.secondary_category_id \
        GROUP BY 1,2"
            
    sql_tuple = execute_query(query_string)
    
    category_totals = {}
    # Make named dictionary of Primary Category buckets / Primary Categories and assign signed amount
    for category in sql_tuple:
        # If Primary Category Bucket has not been initialized, do so, otherwise just define a primary category "underneath" the bucket and enter the signed amount
        # FUTURE: DRY. Is there a way to check whether a bucket dictionary exists without using "try"? Then I don't have to repeat the same line twice
        try:
            category_totals[str(category.primary_category_bucket)][str(category.primary_category)] = category.ed_signed_amount
        except:
            category_totals[str(category.primary_category_bucket)] = SortedDict()
            category_totals[str(category.primary_category_bucket)][str(category.primary_category)] = category.ed_signed_amount
    
    
    return category_totals

def q_ed_sources():

    # Show signed amounts along side Account names (source) and Source Categories
    # Collect Account Source names
    # Collect Account Source categories
    s_sc()
    
    query_string = " \
        SELECT \
            s_sc.source_category \
            , s_sc.source \
            , round(sum(t.transaction_type * t.amount), 2) as signed_amount \
        FROM \
            zillions_transaction t \
        LEFT JOIN \
            s_sc \
        ON \
            t.source_id = s_sc.source_id \
        GROUP BY 1,2"
        
    sql_tuple = execute_query(query_string)
    
    source_totals = SortedDict()
    for category in sql_tuple:
        # If Source Category has not been initialized, do so, otherwise just define a source  "underneath" the category and enter the signed amount
        # FUTURE: DRY. Is there a way to check whether a category dictionary exists without using "try"? Then I don't have to repeat the same line twice
        try:
            source_totals[str(category.source_category)][str(category.source)] = category.signed_amount
        except:
            source_totals[str(category.source_category)] = {}
            source_totals[str(category.source_category)][str(category.source)] = category.signed_amount 

    
    return source_totals

def q_reconciliation():
    # Subtract Ed + Julie total from the account total. If = 0, then reconciled
    # Collect the total signed amount along with signed amounts multiplied by Ed_percent and Julie_percent
    query_string = " \
        SELECT \
            t.sum_signed_amount - (t.ed_signed_amount + t.julie_signed_amount) as reconciliation \
        FROM \
            (SELECT \
                sum(transaction_type * amount) as sum_signed_amount \
                , sum(transaction_type * amount * ed_perc/100) as ed_signed_amount \
                , sum(transaction_type * amount * (1-ed_perc/100)) as julie_signed_amount \
            FROM \
                zillions_transaction) t"
    
    sql_tuple = execute_query(query_string)
    
    reconciliation = sql_tuple[0].reconciliation
    
    return reconciliation

def q_latest_transaction_date():
    
    #Get latest transaction date
    query_string = " \
        SELECT \
            transaction_date \
        FROM \
            zillions_transaction \
        ORDER BY transaction_date DESC \
        LIMIT 1"
        
    sql_tuple = execute_query(query_string)
    
    last_updated = sql_tuple[0].transaction_date

    return last_updated

def q_julie_total():
    #Get Julie Total
    query_string = " \
        SELECT \
            -sum(transaction_type * amount * (1-ed_perc/100)) as julie_signed_amount \
        FROM \
            zillions_transaction"
            
    sql_tuple = execute_query(query_string)
    
    julie_total = sql_tuple[0].julie_signed_amount
    
    return julie_total

def q_new_transactions(perc_bound):
    # Get transactions from the import table that do not have an exact match to transactions from Mint
    
    # Chain:
    #     def s_t_sc():
    #        def t_sc():
    
    s_t_sc()
    
    query_string = " \
        SELECT \
            ti.id \
            , ti.transaction_date \
            , ti.description \
            , ti.original_description \
            , ti.amount \
            , ti.transaction_type \
            , ti.secondary_category \
            , ti.source \
        FROM \
            zillions_transaction_import ti \
        LEFT JOIN \
            s_t_sc \
        ON \
            ti.transaction_date = s_t_sc.transaction_date \
            AND ti.description = s_t_sc.description \
            AND ti.original_description = s_t_sc.original_description \
            AND ti.amount = s_t_sc.amount \
            AND ti.transaction_type = s_t_sc.transaction_type \
            AND ti.secondary_category = s_t_sc.secondary_category \
            AND ti.source = s_t_sc.source \
        WHERE \
            s_t_sc.transaction_id IS NULL;"
        
    sql_tuple = execute_query(query_string)
    
    #FUTURE WIP is not SSOT
    transaction_list = []
    transaction_list_wip = {}
    transaction_list_wip['new'] = []
    transaction_list_wip['dupe'] = []
    
    # Create dictionary for returned transactions
    for row in sql_tuple:
        transaction = {}
        transaction['id'] = row.id
        transaction['transaction_date'] = row.transaction_date
        transaction['description'] = row.description
        transaction['original_description'] = row.original_description
        transaction['amount'] = row.amount
        transaction['transaction_type'] = row.transaction_type
        transaction['secondary_category'] = row.secondary_category
        transaction['source'] = row.source
        
        # Compare imported transactions within perc_bound amount and 10 days
        # FUTURE: Pass in date range along with perc_bound
        lower_transaction_date = str(row.transaction_date - timedelta(days=10))
        upper_transaction_date = str(row.transaction_date + timedelta(days=10))

        lower_amount = row.amount * (1-perc_bound)
        upper_amount = row.amount * (1+perc_bound)
        potential_duplicates = Transaction.objects.filter( Q(transaction_date__range=[lower_transaction_date, upper_transaction_date]), Q(description = row.description) | Q(amount__range=[lower_amount, upper_amount]) )
        
        #Group potential duplicates with the imported transaction
        transaction_group = [transaction, potential_duplicates]
        transaction_list.append(transaction_group)
        
        # Separate imported transactions with no potential duplicates from those with potential duplicates
        if len(potential_duplicates) == 0:
            transaction_list_wip['new'].append(transaction)
        else:
            transaction_list_wip['dupe'].append(transaction_group)
    
    return transaction_list, transaction_list_wip

def q_transaction_list(primary, secondary, source, individual, startdate, enddate, description, amount_lte, amount_gte, transaction_type, incl_internal_transfer, flagged):
    # Get List of transactions given filter criteria and combine with source info
    # Also generate object to be consumed by python/django
    
    # If filtering by category or source, allow for SQL filter term

    # http://stackoverflow.com/questions/6741185/add-quotes-to-every-list-elements
    # https://docs.djangoproject.com/en/dev/topics/db/sql/#performing-raw-sql-queries
    # https://docs.djangoproject.com/en/dev/topics/db/sql/#performing-raw-queries
    # https://docs.djangoproject.com/en/dev/topics/db/sql/#executing-custom-sql-directly
    # https://docs.djangoproject.com/en/dev/topics/security/#sql-injection-protection
    
    # Chain:
    #def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None)
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None) 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None)
    

    #FUTURE be careful here. there is no way, should we eventually chose to, to pass a bucket_name to the transaction_list
    bucket_name = None 

    # Get transaction list and sources
    t_sc_pc_pcb(bucket_name, primary, secondary, startdate, enddate, description, amount_lte, amount_gte, transaction_type, individual, incl_internal_transfer, flagged)

    s(source)

    drop_temp_table('transaction_list')
    # Get spreadsheet view of transactions, including primary / secondary category name, source name, and week/year number
    query_string = " \
        CREATE TEMPORARY TABLE transaction_list as ( \
            SELECT \
                t_sc_pc_pcb.transaction_id \
                , t_sc_pc_pcb.transaction_date \
                , t_sc_pc_pcb.description \
                , round(t_sc_pc_pcb.signed_amount,2) as signed_amount\
                , t_sc_pc_pcb.secondary_category \
                , t_sc_pc_pcb.primary_category \
                , t_sc_pc_pcb.primary_category_bucket \
                , s.name as source_name\
                , round(t_sc_pc_pcb.ed_perc,2) as ed_perc \
                , round(t_sc_pc_pcb.julie_perc,2) as julie_perc \
                , t_sc_pc_pcb.flagged \
                , round(t_sc_pc_pcb.ed_signed_amount,2) as ed_signed_amount\
                , round(t_sc_pc_pcb.julie_signed_amount,2) as julie_signed_amount \
                , t_sc_pc_pcb.year_number \
                , t_sc_pc_pcb.week_number \
            FROM \
                s \
            INNER JOIN \
                t_sc_pc_pcb \
            ON s.id = t_sc_pc_pcb.source_id \
            ORDER BY t_sc_pc_pcb.transaction_date DESC \
        );"
        # http://stackoverflow.com/questions/11146190/python-typeerror-not-enough-arguments-for-format-string
    cursor_execute(query_string)

    query_string = "SELECT * FROM transaction_list;"
    
    sql_tuple = execute_query(query_string)
    transaction_list = []
    
    # Create dictionary for returned transactions
    for row in sql_tuple:
        transaction = {}
        transaction['transaction_id'] = int(row.transaction_id)
        transaction['transaction_date'] = row.transaction_date
        transaction['description'] = row.description
        transaction['signed_amount'] = row.signed_amount
        transaction['secondary_category'] = row.secondary_category
        transaction['primary_category'] = row.primary_category
        transaction['primary_category_category'] = row.primary_category_bucket
        transaction['source_name'] = row.source_name
        transaction['ed_perc'] = row.ed_perc
        transaction['julie_perc'] = row.julie_perc
        transaction['flagged'] = row.flagged
        transaction['ed_signed_amount'] = row.ed_signed_amount
        transaction['julie_signed_amount'] = row.julie_signed_amount
        transaction['week_number'] = row.week_number
        transaction['year_number'] = row.year_number
        transaction_list.append(transaction)
    
    return transaction_list

def q_viz_transaction_list(primary, secondary, source, individual, startdate, enddate, description, amount_lte, amount_gte, transaction_type, incl_internal_transfer, flagged, group_by, interval):
    # Generate a list object that contains transactions with a given filter along with an aggregation of the same transactions
    # **Deprecated** in favor of q_transaction_data_json()
    
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
    
    # Construct SQL query given level of granularity and aggregation required
    
    field_string = " \
        primary_category \
        , SUM(signed_amount) as signed_amount \
        , SUM(ed_signed_amount) as ed_signed_amount \
        , SUM(julie_signed_amount) as julie_signed_amount \
        , year_number"
        
    groupby_string = " \
        primary_category, year_number"
    orderby_string = " \
        ORDER BY year_number DESC"
            
    if group_by == "secondary":
        field_string = field_string + ", secondary_category"
        groupby_string = groupby_string + ", secondary_category"
        
    if interval == "week":
        field_string = field_string + ", week_number"
        groupby_string = groupby_string + ", week_number"
        orderby_string = orderby_string + ", week_number DESC;"
    elif interval == "month":
        field_string = field_string + ", MONTH(transaction_date) as month_number"
        groupby_string = groupby_string = ", month_number"
        orderby_string = orderby_string + ", month_number DESC;"
        
    query_string = "SELECT " + field_string + " FROM transaction_list GROUP BY " + groupby_string + orderby_string

    sql_tuple = execute_query(query_string)
    viz_transaction_list = []
    
    # Create dictionary for returned transactions
    for row in sql_tuple:
        transaction = {}
        
        transaction['primary_category'] = row.primary_category
        transaction['signed_amount'] = row.signed_amount
        transaction['ed_signed_amount'] = row.ed_signed_amount
        transaction['julie_signed_amount'] = row.julie_signed_amount
        transaction['year_number'] = row.year_number
        
        if group_by == "secondary":
            transaction['secondary_category'] = row.secondary_category
        if interval == "week":
            transaction['week_number'] = row.week_number
        elif  interval == "month":
            transaction['month_number'] = row.month_number
        viz_transaction_list.append(transaction)
    
    return transaction_list, viz_transaction_list

def q_julie_amortization_total():
    #Get Julie Total: julie_signed_amount
    
    bucket_name = 'Debt'
    primary_names = None
    secondary_names = None
    
    # Chain:
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None) 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None)

    
    sc_pc_pcb(bucket_name, primary_names, secondary_names)

    
    query_string = " \
            SELECT \
                -1 * round(sum(t.transaction_type * t.amount * (1-t.ed_perc/100)), 2) as julie_signed_amount \
            FROM \
                zillions_transaction t \
            INNER JOIN \
                sc_pc_pcb \
            ON t.secondary_category_id = sc_pc_pcb.secondary_category_id;"
    
    sql_tuple = execute_query(query_string)
    
    julie_amort_total = sql_tuple[0].julie_signed_amount

    return julie_amort_total


def sum_weekly_spend(ed_weight, julie_weight, startdate, enddate):
    # Get sum of weekly spend including dummy weeks for Julie, Ed, or both
    
    dw_sc_pc_pcb(startdate, enddate)
    
    drop_temp_table('sum_weekly_spend')
    
    query_string = " \
        CREATE TEMPORARY TABLE sum_weekly_spend as ( \
            /* Get sum of weekly spend within date range for every secondary transaction with 0's filled for missing weeks */ \
            SELECT \
                dw_week_number \
                , dw_year_number \
                , primary_category \
                , primary_category_bucket \
                , primary_category_id \
                , secondary_category \
                , dwsc.secondary_category_id \
                , CASE \
                    WHEN weekly_spend is NULL then 0 \
                    ELSE weekly_spend \
                    END as weekly_spend \
            FROM \
                \
                /* Get sum of weekly spend for all secondary transactions within date range */ \
                (SELECT \
                    secondary_category_id \
                    , CASE \
                        WHEN WEEK(transaction_date,2) > WEEK(transaction_date) then YEAR(transaction_date) - 1 \
                        ELSE YEAR(transaction_date) \
                        END as sum_calc_year_number \
                    , WEEK(transaction_date,2) as sum_calc_week_number \
                    , -sum(transaction_type * amount * ed_perc/100 * %s + transaction_type * amount * (1- ed_perc/100) * %s) as weekly_spend \
                FROM \
                    zillions_transaction \
                WHERE \
                    transaction_date >= '%s' \
                    AND transaction_date <= '%s' \
                    AND internal_transfer = 0 \
                GROUP BY \
                    secondary_category_id, sum_calc_week_number, sum_calc_year_number) as sum_calc \
            RIGHT JOIN \
                (SELECT \
                    year_number as dw_year_number \
                    , week_number as dw_week_number \
                    , primary_category \
                    , primary_category_bucket \
                    , primary_category_id \
                    , secondary_category \
                    , secondary_category_id \
                FROM \
                    dw_sc_pc_pcb) as dwsc \
            ON \
                sum_calc.sum_calc_year_number = dwsc.dw_year_number \
                AND sum_calc.sum_calc_week_number = dwsc.dw_week_number \
                AND sum_calc.secondary_category_id = dwsc.secondary_category_id \
        );" % (ed_weight, julie_weight, startdate, enddate)
        
    return cursor_execute(query_string)



def scma(startdate, enddate, moving_avg_weeks, individual):
    #Calculate moving averages for weekly spend by secondary category
    
    # Determine number of weeks of moving averages needed
    delta = enddate - startdate
    num_weeks = (delta.days + 1) / 7
    
    drop_temp_table('sc_moving_avg')
    # Create template table
    query_string = " \
        CREATE TEMPORARY TABLE sc_moving_avg ( \
            year_number integer \
            , week_number integer \
            , secondary_category_id integer \
            , moving_avg numeric(9,4) \
            ); \
        "
    cursor = connection.cursor()
    cursor.execute(query_string)

    #Determine whether to pull spend for ed, julie or both
    ed_weight, julie_weight = erjs_weights(individual)
            
    #Get avg weekly spend over time horizon per person per category 
    for i in range(0, num_weeks):
        #Move the start date and end date for every iteration
        moving_avg_enddate = enddate - timedelta(days=i * 7)
        moving_avg_startdate = enddate - timedelta(days=moving_avg_weeks * 7 + i * 7 - 1)
        
        #Get total spend per category per week per person over given time horizon
        sum_weekly_spend(ed_weight, julie_weight, moving_avg_startdate, moving_avg_enddate)
        
        # Moving Average for the date range defined with 0's filled in for missing weeks
        query_string = " \
            SELECT \
                CASE \
                    WHEN WEEK('%s',2) > WEEK('%s') then YEAR('%s') - 1 \
                    ELSE YEAR('%s') \
                    END as year_number \
                , WEEK('%s',2) as week_number \
                , secondary_category_id \
                , round(avg(weekly_spend),4) as moving_avg \
            FROM \
                sum_weekly_spend \
            GROUP BY \
                year_number, week_number, secondary_category_id \
        " % (moving_avg_enddate, moving_avg_enddate, moving_avg_enddate, moving_avg_enddate, moving_avg_enddate)

        sql_tuple = execute_query(query_string)

        # Insert moving average per secondary category for week into a table
        for secondary_category in sql_tuple:
            query_string = " \
                INSERT INTO sc_moving_avg (year_number, week_number, secondary_category_id, moving_avg) \
                VALUES (%s, %s, %s, %s); \
                " % (secondary_category.year_number, secondary_category.week_number, secondary_category.secondary_category_id, secondary_category.moving_avg)
            cursor = connection.cursor()
            cursor.execute(query_string)

def erjs_weights(individual):
    #Set weighting on ed or julie (or both) dependent on what individual is being filtered for
    ed_weight = 1
    julie_weight = 1
    if individual:
        if individual == "ed":
            # Zero out Julie's contribution to spending
            julie_weight = 0
        elif individual == "julie":
            # Zero out Ed's contribution to spending
            ed_weight = 0  
    return ed_weight, julie_weight


def w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
    # Get spend by week alongside moving average by secondary category
    
    # Determine if we should filter out an individual's spending contribution 
    ed_weight, julie_weight = erjs_weights(individual)
    
    # Chains
    #    def scma(startdate, enddate, moving_avg_weeks, individual):
            #Calculate moving averages for weekly spend by secondary category
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None):
    
    #FUTURE: Pull filter criteria into this function call if necessary
    sc_pc_pcb()

    # Get secondary category moving averages
    scma(startdate, enddate, moving_avg_weeks, individual)
    
    # Get sum of weekly spend with 0 filled for empty weeks
    sum_weekly_spend(ed_weight, julie_weight, startdate, enddate)
    
    drop_temp_table('w_scma_t_sc_pc_pcb')
    
    query_string = " \
        CREATE TEMPORARY TABLE w_scma_t_sc_pc_pcb as ( \
            SELECT \
                dw_w_t_sc_pc_pcb.year_number \
                , dw_w_t_sc_pc_pcb.week_number \
                , dw_w_t_sc_pc_pcb.primary_category \
                , dw_w_t_sc_pc_pcb.primary_category_bucket \
                , dw_w_t_sc_pc_pcb.primary_category_id \
                , dw_w_t_sc_pc_pcb.secondary_category \
                , dw_w_t_sc_pc_pcb.secondary_category_id \
                , dw_w_t_sc_pc_pcb.sc_week_spend \
                , scma.moving_avg \
            FROM \
                (SELECT \
                    dw_year_number as year_number \
                    , dw_week_number as week_number \
                    , primary_category \
                    , primary_category_bucket \
                    , primary_category_id \
                    , secondary_category \
                    , secondary_category_id \
                    , CASE \
                        WHEN weekly_spend is NULL then 0 \
                        ELSE weekly_spend \
                        END as sc_week_spend \
                FROM \
                    sum_weekly_spend) as dw_w_t_sc_pc_pcb \
            INNER JOIN \
                sc_moving_avg as scma \
            ON \
                dw_w_t_sc_pc_pcb.year_number = scma.year_number \
                AND dw_w_t_sc_pc_pcb.week_number = scma.week_number \
                AND dw_w_t_sc_pc_pcb.secondary_category_id = scma.secondary_category_id \
           );"                

    return cursor_execute(query_string)


def b(individual):
    # Get budget amounts by secondary_category
    
    drop_temp_table('b')
    
    # FUTURE: This is a hack!!!
    if individual is None:
        individual = "ed"
    
    ed_weight, julie_weight = erjs_weights(individual)
    rockleton_id = Rockleton.objects.filter(user__first_name__icontains=individual)[0].id
            
    query_string = " \
        CREATE TEMPORARY TABLE b as ( \
            SELECT \
                secondary_category_id \
                , CASE \
                    WHEN amount is NULL then 0 \
                    WHEN time_period = 'WE' then (amount * ed_perc/100 * %s + amount * (1-ed_perc/100) * %s) \
                    ELSE (amount * ed_perc/100 * %s + amount * (1-ed_perc/100) * %s) / 4 \
                    END as sc_budgeted_amount \
            FROM \
                (SELECT \
                    rsc.id as secondary_category_id\
                    , rb.time_period \
                    , rb.amount \
                    , rb.ed_perc \
                FROM \
                    zillions_secondary_category rsc \
                LEFT JOIN \
                    (SELECT \
                        * \
                    FROM \
                        zillions_budget \
                    WHERE \
                        user_id = %s) rb \
                ON \
                    rsc.id = rb.secondary_category_id) rsc_rb \
           );" % (ed_weight, julie_weight, ed_weight, julie_weight, rockleton_id)

    return cursor_execute(query_string)


def b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual, primary = ""):
    # Get weekly spend alongside moving average alongside budget amount per secondary category
    
    # Chain:
    #def w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
    #    sc_pc_pcb()
    #    def scma(startdate, enddate, moving_avg_weeks, individual):
    #        #Calculate moving averages for weekly spend by secondary category
    #def b(individual):
    
    # Get weekly spend alongside moving average
    w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual)
    b(individual)
    
    if primary is not "":
        primary = "WHERE primary_category = '" + primary + "'"
        
    
    drop_temp_table('b_w_scma_t_sc_pc_pcb')
    
    query_string = " \
        CREATE TEMPORARY TABLE b_w_scma_t_sc_pc_pcb AS ( \
                SELECT \
                    f_w_scma_t_sc_pc_pcb.year_number \
                    , f_w_scma_t_sc_pc_pcb.week_number \
                    , f_w_scma_t_sc_pc_pcb.primary_category \
                    , f_w_scma_t_sc_pc_pcb.primary_category_id \
                    , f_w_scma_t_sc_pc_pcb.primary_category_bucket \
                    , f_w_scma_t_sc_pc_pcb.secondary_category \
                    , f_w_scma_t_sc_pc_pcb.secondary_category_id \
                    , round(f_w_scma_t_sc_pc_pcb.sc_week_spend,2) as sc_week_spend \
                    , round(b.sc_budgeted_amount,2) as sc_budgeted_amount \
                    , f_w_scma_t_sc_pc_pcb.moving_avg as sc_moving_avg \
                    , round(((b.sc_budgeted_amount - f_w_scma_t_sc_pc_pcb.sc_week_spend)/b.sc_budgeted_amount)*100,1) as sc_perc_remaining \
                    , round(((b.sc_budgeted_amount - f_w_scma_t_sc_pc_pcb.moving_avg)/b.sc_budgeted_amount)*100,1) as sc_perc_remaining_moving_avg \
                FROM \
                    (SELECT * FROM w_scma_t_sc_pc_pcb %s) f_w_scma_t_sc_pc_pcb \
                INNER JOIN \
                    b \
                ON f_w_scma_t_sc_pc_pcb.secondary_category_id = b.secondary_category_id \
                ORDER BY \
                    f_w_scma_t_sc_pc_pcb.primary_category_bucket \
                    , f_w_scma_t_sc_pc_pcb.primary_category \
                    , f_w_scma_t_sc_pc_pcb.secondary_category \
                    , f_w_scma_t_sc_pc_pcb.year_number, f_w_scma_t_sc_pc_pcb.week_number \
            ); \
            " % primary

    return cursor_execute(query_string)



def tot_w_pc(individual):
    # Get sum of transactions by primary category alongside weekly spend / budgets grouped by primary category as well
    
    # Chains:
    #def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None):
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None):
    
    # Get all the transactions. Yes, ALL
    t_sc_pc_pcb()
    drop_temp_table('tot_w_pc')
    
    ed_weight, julie_weight = erjs_weights(individual)
            
    
    query_string = " \
        CREATE TEMPORARY TABLE tot_w_pc AS ( \
            SELECT \
                w_pc_b_w_scma_t_sc_pc_pcb.year_number \
                , w_pc_b_w_scma_t_sc_pc_pcb.week_number \
                , w_pc_b_w_scma_t_sc_pc_pcb.primary_category \
                , w_pc_b_w_scma_t_sc_pc_pcb.primary_category_bucket \
                , w_pc_b_w_scma_t_sc_pc_pcb.primary_category_id \
                , round(w_pc_b_w_scma_t_sc_pc_pcb.pc_week_spend,2) as pc_week_spend \
                , round(w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount,2) as pc_budgeted_amount \
                , round(tot_t_sc_pc_pcb.pc_total_surplus,2) as pc_total_surplus \
                , round(w_pc_b_w_scma_t_sc_pc_pcb.pc_moving_avg,2) as pc_moving_avg \
                , round((tot_t_sc_pc_pcb.pc_total_surplus / w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount),1) as pc_surplus_multiples_of_budget \
                , round(((w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount - w_pc_b_w_scma_t_sc_pc_pcb.pc_week_spend)/w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount)*100,1) as pc_perc_remaining \
                , round(((w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount - w_pc_b_w_scma_t_sc_pc_pcb.pc_moving_avg)/w_pc_b_w_scma_t_sc_pc_pcb.pc_budgeted_amount)*100,1) as pc_perc_remaining_moving_avg \
            FROM \
                (SELECT \
                    b_w_scma_t_sc_pc_pcb.year_number \
                    , b_w_scma_t_sc_pc_pcb.week_number \
                    , b_w_scma_t_sc_pc_pcb.primary_category \
                    , b_w_scma_t_sc_pc_pcb.primary_category_id \
                    , b_w_scma_t_sc_pc_pcb.primary_category_bucket \
                    , sum(b_w_scma_t_sc_pc_pcb.sc_week_spend) as pc_week_spend \
                    , sum(b_w_scma_t_sc_pc_pcb.sc_budgeted_amount) as pc_budgeted_amount \
                    , sum(b_w_scma_t_sc_pc_pcb.sc_moving_avg) as pc_moving_avg \
                FROM \
                    b_w_scma_t_sc_pc_pcb \
                GROUP BY \
                    b_w_scma_t_sc_pc_pcb.year_number \
                    , b_w_scma_t_sc_pc_pcb.week_number \
                    , b_w_scma_t_sc_pc_pcb.primary_category_id) w_pc_b_w_scma_t_sc_pc_pcb \
            INNER JOIN \
                (SELECT \
                    primary_category_id \
                    , sum(ed_signed_amount) * %s + sum(julie_signed_amount) * %s as pc_total_surplus \
                FROM \
                    t_sc_pc_pcb \
                GROUP BY \
                    primary_category_id) tot_t_sc_pc_pcb \
            ON \
                w_pc_b_w_scma_t_sc_pc_pcb.primary_category_id = tot_t_sc_pc_pcb.primary_category_id \
            ORDER BY \
                w_pc_b_w_scma_t_sc_pc_pcb.primary_category_bucket \
                , w_pc_b_w_scma_t_sc_pc_pcb.primary_category \
                , w_pc_b_w_scma_t_sc_pc_pcb.year_number, w_pc_b_w_scma_t_sc_pc_pcb.week_number \
            ); \
            " % (ed_weight, julie_weight)
            
    
    return cursor_execute(query_string)

def convert_to_float(var):
    #JSON serializer requires decimals to be floats
    float_var = float(0)
    if var:
        float_var = float(var)

    return float_var

#FUTURE
#@csrf_exempt
def q_budget_view_json(request):
    # Send spend summary to view using javascript
    # spend vs budget per week for every SC / PC along with moving averages
    
    # Chains:
    #def b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
        # Get weekly spend alongside moving average
    #    def w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
    #        def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #            def pc_pcb(bucket_name, primary_names)
    #                def pcb(bucket_name = None):
    #        def scma(startdate, enddate, moving_avg_weeks, individual):
    #            #Calculate moving averages for weekly spend by secondary category
    #    def b(individual):
    
    #def tot_w_pc(individual):
    #    def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None):
    #        def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #            def pc_pcb(bucket_name, primary_names)
    #                def pcb(bucket_name = None):
    
    # Chains:
    #def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None):
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None):
    

    # Not putting try/except here because the view sends start and end values to the template
    # The views.py sends the parameters to the template. But this view is called from the javascript, so the try's shouldn't be needed
    # there is no try. there is only do or error out
    startdate = datetime.strptime(request.GET['startdate'], '%Y%m%d').date()
    enddate = datetime.strptime(request.GET['enddate'], '%Y%m%d').date()

    # Calculate start and end dates as of the beginning and end of weeks
    startdate = startdate - timedelta(days=(startdate.weekday() + 1) % 7)
    enddate = enddate - timedelta(days=(enddate.weekday() + 1) % 7) + timedelta(days=6)
    
    #Determine the number of weeks over which to calc the moving average
    moving_avg_weeks = int(request.GET['moving_avg_weeks'])
    individual = request.GET['individual']
     
    # Get secondary category spend / moving average vs budget
    b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual)
    # Get primary category surplus and aggregate over secondary categories
    tot_w_pc(individual)
    
    query_string = "SELECT * FROM b_w_scma_t_sc_pc_pcb;"
    #SC-level detail
    sc_sql_tuple = execute_query(query_string)
    
    query_string = "SELECT * FROM tot_w_pc;"
    #PC-level detail
    pc_sql_tuple = execute_query(query_string)

    #Populate the primary category data
    pc_category_totals = {}
    for category in pc_sql_tuple:
        
        #This is top level information about the primary category
        #The perc_remaining/multiples of budget will be replaced each iteration and will ultimately represent the latest week's perc_remaining/multiples of budget
        primary_category_dict = {
            'primary_category'                  : category.primary_category,
            'pc_surplus_multiples_of_budget'    : convert_to_float(category.pc_surplus_multiples_of_budget),
            'pc_total_surplus'                  : convert_to_float(category.pc_total_surplus),
            'pc_perc_remaining'                 : convert_to_float(category.pc_perc_remaining),
            'pc_secondary_category_data'        : {},
            'primary_category_data'             : [],
            }

        # This is the weekly view of the primary category data
        primary_category_data = {
            'week_number'                       : category.week_number,
            'pc_perc_remaining'                 : convert_to_float(category.pc_perc_remaining),
            'pc_perc_remaining_moving_avg'      : convert_to_float(category.pc_perc_remaining_moving_avg),
            'pc_budgeted_amount'                : convert_to_float(category.pc_budgeted_amount),
            'pc_week_spend'                     : convert_to_float(category.pc_week_spend)
            }

        #Initialize dictionary for a particular primary category bucket
        if str(category.primary_category_bucket) not in pc_category_totals:
            pc_category_totals[str(category.primary_category_bucket)] = SortedDict()
        
        #Initialize dictionary for a particular primary category
        if str(category.primary_category) not in pc_category_totals[str(category.primary_category_bucket)]:
            pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)] = primary_category_dict

            
        #Push another week's data for primary category
        pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['primary_category_data'].append(primary_category_data)
        #Replace the top level multiples and perc_remaining values
        pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['pc_surplus_multiples_of_budget'] = convert_to_float(category.pc_surplus_multiples_of_budget)
        pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['pc_perc_remaining'] = convert_to_float(category.pc_perc_remaining)

    #Populate the secondary category data
    for category in sc_sql_tuple:

        secondary_category_data = {
            'secondary_category'            : category.secondary_category,
            'week_number'                   : category.week_number,
            #'year_number'                  : category.year_number,
            #'sc_week_spend'                : category.sc_week_spend,
            #'sc_budgeted_amount'           : category.sc_budgeted_amount,
            'sc_perc_remaining'             : convert_to_float(category.sc_perc_remaining),
            'sc_perc_remaining_moving_avg'  : convert_to_float(category.sc_perc_remaining_moving_avg),
            #'sc_moving_avg'                 : category.sc_moving_avg,
        }
        
        #Initialize dictionary for a particular secondary category
        if str(category.secondary_category) not in pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['pc_secondary_category_data']:
            pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['pc_secondary_category_data'][str(category.secondary_category)] = []

        #Push another week's data for secondary category
        pc_category_totals[str(category.primary_category_bucket)][str(category.primary_category)]['pc_secondary_category_data'][str(category.secondary_category)].append(secondary_category_data)

    json_data = json.dumps(pc_category_totals, cls=DjangoJSONEncoder)


    return HttpResponse(json_data,'application/json')

#FUTURE
#@csrf_exempt
def q_budget_summary_json(individual):
    #Double of q_ed_primary_buckets() to get extra data
    # This is for the index page
    # balance per PC along with current week's spend vs budget
    
    # I would prefer to make queries using django's terminology
    # But I need to do some data transformations before I aggregate
    # I need to multiply transaction_type (-1,1) by amount before I sum by category
    # I can't find a way to do this
    # The below code is as close as I got
    # I included a @property in the model to get the signed_amount, but there's no way to annotate by a property
    # So for now, I'm going to write Raw SQL
    
    # Primary Category Totals for Ed
    # Show signed amounts multiplied by Ed Percent along side Primary Category Bucket and Primary Category Names
    # Collection Secondary Category ids to join to transactions
    # Collect Primary Category Names
    # Collect Primary Category buckets
    
    # Chains
    #def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None)
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None) 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None)
    
    t_sc_pc_pcb(individual = individual)  
    
    ed_weight, julie_weight = erjs_weights(individual)
    
    # Get balance per PC
    query_string = """
        CREATE TEMPORARY TABLE q_t_pc_pcb as (
            SELECT
                primary_category
                , primary_category_id
                , primary_category_bucket
                , round(sum(ed_signed_amount * %s + julie_signed_amount * %s),2) as signed_amount
            FROM
                t_sc_pc_pcb
            GROUP BY 1,2,3);
        """ % (ed_weight, julie_weight)
    cursor_execute(query_string)
    
    dt = date.today()
    first_day_of_current_week = dt - timedelta(days=(dt.weekday() + 1) % 7)
    
    t_sc_pc_pcb(individual = individual, startdate = first_day_of_current_week, incl_internal_transfer = 0)
    
    sc_pc_pcb()
        
    # Get current week spend per PC
    query_string = """ 
        CREATE TEMPORARY TABLE q_t_pc_pcb_week as (
            SELECT
                primary_category
                , primary_category_id
                , primary_category_bucket
                , CASE
                    WHEN cw_spend is null then 0
                    WHEN cw_spend is not null then cw_spend
                    END as cw_spend
            FROM
                (SELECT 
                    sc_pc_pcb.primary_category 
                    , sc_pc_pcb.primary_category_id 
                    , sc_pc_pcb.primary_category_bucket 
                    , round(sum(ed_signed_amount * %s + julie_signed_amount * %s),2) as cw_spend 
                 FROM
                    t_sc_pc_pcb
                RIGHT JOIN 
                    sc_pc_pcb 
                ON 
                    t_sc_pc_pcb.secondary_category_id = sc_pc_pcb.secondary_category_id 
                 GROUP BY 1,2,3) tbl);
        """ % (ed_weight, julie_weight)
    cursor_execute(query_string)

    #print q_t_pc_pcb_week
    
    #rockleton_id = Rockleton.objects.filter(user__first_name__icontains=individual)[0].id
    b(individual)
            
    # Get budget per PC
    query_string = """
        CREATE TEMPORARY TABLE q_b_pc_pcb as (
            SELECT
                primary_category
                , primary_category_id
                , round(sum(sc_budgeted_amount),2) as pc_budgeted_amount
            FROM
                (SELECT
                     primary_category
                     , primary_category_id
                     , sc_budgeted_amount
                 FROM
                     sc_pc_pcb
                 LEFT JOIN
                     b
                 ON
                     sc_pc_pcb.secondary_category_id = b.secondary_category_id) b_sc_pc_pcb
            GROUP BY 1,2);
        """
    cursor_execute(query_string)
    
    # Grand JOIN
    q_b_t_pc_pcb = """
        SELECT
            q_t_pc_pcb.primary_category
            , q_t_pc_pcb.primary_category_bucket
            , q_b_pc_pcb.pc_budgeted_amount
            , q_t_pc_pcb.signed_amount
            , q_t_pc_pcb_week.cw_spend + q_b_pc_pcb.pc_budgeted_amount as signed_amount_current_week
        FROM
            q_b_pc_pcb
        INNER JOIN
            q_t_pc_pcb
        ON
            q_b_pc_pcb.primary_category_id = q_t_pc_pcb.primary_category_id
        INNER JOIN
            q_t_pc_pcb_week
        ON
            q_b_pc_pcb.primary_category_id = q_t_pc_pcb_week.primary_category_id
        ;"""
        
    #print q_b_t_pc_pcb
    
    sql_tuple = execute_query(q_b_t_pc_pcb)
    
    category_totals = {}
    # Make named dictionary of Primary Category buckets / Primary Categories and assign signed amount
    for category in sql_tuple:
        # If Primary Category Bucket has not been initialized, do so, otherwise just define a primary category "underneath" the bucket and enter the signed amount
        # FUTURE: DRY. Is there a way to check whether a bucket dictionary exists without using "try"? Then I don't have to repeat the same line twice
        try:
            define_summary_json(category_totals, category)
        except:

            category_totals[str(category.primary_category_bucket)] = SortedDict()
            
            define_summary_json(category_totals, category)
    #print category_totals
    
    return category_totals

def define_summary_json(category_totals, category):
    # Balance, current week totals, and viz bar definitions per PC
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)] = {}
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["total"] = category.signed_amount
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_total"] = category.signed_amount_current_week
    
    # Define a budget of 1 if budget = 0
    # This helps with divide by 0 calcs on the viz bar lengths
    if category.pc_budgeted_amount == 0:
        pragmatic_pc_budgeted_amount = 1
    else:
        pragmatic_pc_budgeted_amount = category.pc_budgeted_amount
    
    # # length of Red/Very Red and green/very green bars in test ajax budget summary
    bar_params = get_bar_params(pragmatic_pc_budgeted_amount, category.signed_amount)

    # Add data to dictionary for UI consumption for PC balance
    # bar will either extend left for negative or right for positive
    # negative = red, positive = green
    # Each bar needs to know where to start and how long to be
    # pp = Dark red, nn = dark green. Both indicate extremes of negative or positive
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["bar_color"] = bar_params["bar_color"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["negative_budget_bar"] = bar_params["negative_budget_bar"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["positive_budget_bar"] = bar_params["positive_budget_bar"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["p_bar_length"] = bar_params["p_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["p_bar_start"] = bar_params["p_bar_start"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["pp_bar_length"] = bar_params["pp_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["pp_bar_start"] = bar_params["pp_bar_start"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["n_bar_length"] = bar_params["n_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["n_bar_start"] = bar_params["n_bar_start"]

    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["nn_bar_length"] = bar_params["nn_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["nn_bar_start"] = bar_params["nn_bar_start"]
    
    # Add data to dictionary for UI consumption for PC current week balance
    bar_params = get_bar_params(pragmatic_pc_budgeted_amount, category.signed_amount_current_week)
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_bar_color"] = bar_params["bar_color"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_negative_budget_bar"] = bar_params["negative_budget_bar"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_positive_budget_bar"] = bar_params["positive_budget_bar"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_p_bar_length"] = bar_params["p_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_p_bar_start"] = bar_params["p_bar_start"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_pp_bar_length"] = bar_params["pp_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_pp_bar_start"] = bar_params["pp_bar_start"]
    
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_n_bar_length"] = bar_params["n_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_n_bar_start"] = bar_params["n_bar_start"]

    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_nn_bar_length"] = bar_params["nn_bar_length"]
    category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["cw_nn_bar_start"] = bar_params["nn_bar_start"]


    return category_totals

def get_bar_params(budgeted_amount, signed_amount):
    # length of Red/Very Red and green/very green bars in test ajax budget summary
    if budgeted_amount == 0:
        # Deprecated because we don't allow budget to = 0
        p_bar_start = 0
        if signed_amount >= 0:
            bar_color = "rgb(0,255,0)"
        else:
            bar_color = "rgb(255,0,0)"
        negative_budget_bar = 0
        p_bar_length = 100
        positive_budget_bar = 99
        
        pp_bar_length = 0
        pp_bar_start = 0
        nn_bar_length = 0
        nn_bar_start = 0
        n_bar_length = 0
        n_bar_start = 0
    else:
        #if spend is positive
        if signed_amount >= 0:
            # bar starts in the middle of the range
            p_bar_start = 50
            bar_color = "rgb(0,255,0)"
            
            # default the red marker to 0 (out of 100)
            negative_budget_bar = 0
            
            nn_bar_length = 0
            nn_bar_start = 0
            n_bar_length = 0
            n_bar_start = 0

            
            if signed_amount >= budgeted_amount:
                # If overgreen
                
                # Calc overall length of all green along with lengths of green/dark green as multiples of the budgeted amount
                # Determine where to place marker (from 50-100)
                positive_budget_bar = Decimal(budgeted_amount/signed_amount) * Decimal(49) + Decimal(49)
                p_bar_length = Decimal(budgeted_amount/signed_amount) * Decimal(49) + Decimal(49)
                pp_bar_length = 49 - Decimal(budgeted_amount/signed_amount) * Decimal(49) + Decimal(49)
                pp_bar_start = p_bar_length
            else:
                
                #category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["bar_length"] = Decimal(category.ed_signed_amount/category.pc_budgeted_amount) * Decimal(50)
                # If signed amount < budgeted amount, put the marker at "100" (99)
                positive_budget_bar = 99 
                # The bar length is "50"
                p_bar_length = Decimal(signed_amount/budgeted_amount) * Decimal(49)
            
                # No dark green bar
                pp_bar_length = 0
                pp_bar_start = 0

            
        else:
            # signed amount is negative, draw green
            bar_color = "rgb(255,0,0)"
            
            # positive marker is at "100"
            positive_budget_bar = 99
            
            # no green bars
            p_bar_start = 0
            p_bar_length = 0
            pp_bar_length = 0
            pp_bar_start = 0
            
            if signed_amount <= -budgeted_amount:
                # if overred
                #category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["bar_length"] = 50
                #category_totals[str(category.primary_category_bucket)][str(category.primary_category)]["bar_start"] = 0
                
                # Determine where to place marker (from 0-50)
                negative_budget_bar = 50 - Decimal(-budgeted_amount/signed_amount) * Decimal(50)
                # determine lengths of red and dark red bars
                nn_bar_length = 50 - Decimal(-budgeted_amount/signed_amount) * Decimal(50)
                nn_bar_start = 0
                n_bar_length = Decimal(-budgeted_amount/signed_amount) * Decimal(50)
                n_bar_start = nn_bar_length
            else:
                # If spend isn't over red, put the marker at 0 and calc other legnths
                n_bar_length = Decimal(-signed_amount/budgeted_amount) * Decimal(50)
                n_bar_start = 50 - Decimal(-signed_amount/budgeted_amount) * Decimal(50)
                negative_budget_bar = 0
                
                nn_bar_length = 0
                nn_bar_start = 0

    bar_params = {
        'p_bar_start' : p_bar_start
        , 'bar_color' : bar_color
        , 'negative_budget_bar' : negative_budget_bar
        , 'p_bar_length' : p_bar_length
        , 'positive_budget_bar' : positive_budget_bar
        , 'pp_bar_length' : pp_bar_length
        , 'pp_bar_start' : pp_bar_start
        , 'nn_bar_length' : nn_bar_length
        , 'nn_bar_start' : nn_bar_start
        , 'n_bar_length' : n_bar_length
        , 'n_bar_start' : n_bar_start
    }


    return bar_params

def q_ed_pc_total(primary_names, individual):
    # Query balance per specific primary category
    
    # Get transactions given filters
    t_sc_pc_pcb(primary_names = primary_names, individual = individual)  
    
    # Chains
    #def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None)
    #    def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None) 
    #        def pc_pcb(bucket_name, primary_names)
    #            def pcb(bucket_name = None)
    
    ed_weight, julie_weight = erjs_weights(individual)

    q_pc_t = """
        SELECT
            round(sum(ed_signed_amount * %s + julie_signed_amount * %s),2) as signed_amount
        FROM
            t_sc_pc_pcb
        """ % (ed_weight, julie_weight)
        
    sql_tuple = execute_query(q_pc_t)
    
    pc_t = sql_tuple[0].signed_amount

    return pc_t

def q_transaction_data_json(request):
    # get list of transactions per filter as well as spend per certain interval either vs a budget or absolute for all or Ed vs Julie
    # Show viz given transaction list
    
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
        startdate = datetime.strptime(request.GET['startdate'], '%Y%m%d').date()
    except:
        # Default to 6 weeks earlier
        startdate = date.today() - timedelta(days=42)     

    try:
        enddate = datetime.strptime(request.GET['enddate'], '%Y%m%d').date()
    except:
        # Default to today
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
        
    try:
        group_by = request.GET['group_by']
    except:
        group_by = ''
        
    try:
        interval = request.GET['interval']
    except:
        interval = ''
        
    #Number of weeks over which to calculate the moving average
    try:
        moving_avg_weeks = int(request.GET['moving_avg_weeks'])
    except:
        moving_avg_weeks = 6    

    # Calculate start and end dates as of the beginning and end of weeks
    #REDO this based on the interval parameter. If week, this is fine. If month? year?
    startdate = startdate - timedelta(days=(startdate.weekday() + 1) % 7)
    enddate = enddate - timedelta(days=(enddate.weekday() + 1) % 7) + timedelta(days=6)
    
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
    
    # Construct SQL query given level of granularity and aggregation required
    
    field_string = " \
        primary_category \
        , SUM(signed_amount) as signed_amount \
        , SUM(ed_signed_amount) as ed_signed_amount \
        , SUM(julie_signed_amount) as julie_signed_amount \
        , year_number"
        
    groupby_string = " \
        primary_category, year_number"
    orderby_string = " \
        ORDER BY year_number DESC"
            
    if group_by == "secondary":
        field_string = field_string + ", secondary_category"
        groupby_string = groupby_string + ", secondary_category"
        
    if interval == "week":
        field_string = field_string + ", week_number"
        groupby_string = groupby_string + ", week_number"
        orderby_string = orderby_string + ", week_number DESC;"
    elif interval == "month":
        field_string = field_string + ", MONTH(transaction_date) as month_number"
        groupby_string = groupby_string = ", month_number"
        orderby_string = orderby_string + ", month_number DESC;"
        
    query_string = "SELECT " + field_string + " FROM transaction_list GROUP BY " + groupby_string + orderby_string

    sql_tuple = execute_query(query_string)
    
    viz_transaction_list = []
    
    # Create dictionary for returned transactions
    # This function is deprecated in favor of build_viz() below
    for row in sql_tuple:
        transaction = {}
        
        transaction['primary_category'] = row.primary_category
        transaction['signed_amount'] = row.signed_amount
        transaction['ed_signed_amount'] = row.ed_signed_amount
        transaction['julie_signed_amount'] = row.julie_signed_amount
        transaction['year_number'] = row.year_number
        
        if group_by == "secondary":
            transaction['secondary_category'] = row.secondary_category
        if interval == "week":
            transaction['week_number'] = row.week_number
        elif  interval == "month":
            transaction['month_number'] = row.month_number
        viz_transaction_list.append(transaction)
        
    # Get table in format consumable by D3
    viz_transaction_list = build_viz(startdate, enddate, moving_avg_weeks, individual, primary, secondary)
    
    transaction_data = [transaction_list, viz_transaction_list]

    # pass json to the UI
    json_data = json.dumps(transaction_data, cls=DjangoJSONEncoder)

    return HttpResponse(json_data,'application/json')

def build_viz(startdate, enddate, moving_avg_weeks, individual, primary, secondary):
    # get spend per certain interval either vs a budget or absolute for all or Ed vs Julie
    
    # Chains:
    #def b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
        # Get weekly spend alongside moving average
    #    def w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual):
    #        def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #            def pc_pcb(bucket_name, primary_names)
    #                def pcb(bucket_name = None):
    #        def scma(startdate, enddate, moving_avg_weeks, individual):
    #            #Calculate moving averages for weekly spend by secondary category
    #    def b(individual):
    
    #def tot_w_pc(individual):
    #    def t_sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None, startdate = None, enddate = None, description = '', amount_lte = None, amount_gte = None, transaction_type = None, individual = None, incl_internal_transfer = 1, flagged = None):
    #        def sc_pc_pcb(bucket_name = None, primary_names = None, secondary_names = None): 
    #            def pc_pcb(bucket_name, primary_names)
    #                def pcb(bucket_name = None):
    
    # Get secondary category spend / moving average vs budget
    b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, individual, primary)
    # Get primary category surplus and aggregate over secondary categories
    tot_w_pc(individual)
    
    query_string = "SELECT * FROM b_w_scma_t_sc_pc_pcb;"
    #SC-level detail
    sc_sql_tuple = execute_query(query_string)
    
    query_string = "SELECT * FROM tot_w_pc;"
    #PC-level detail
    pc_sql_tuple = execute_query(query_string)
    
    if individual is None:
        # if not pulling total, split spend by Ed and Julie amounts 
        individual_list = ['ed','julie']
        specific_sc_sql_tuple = {}
        specific_pc_sql_tuple = {}
        for person in individual_list:
            # get spend vs budget and absolute by person
            drop_temp_table('b_w_scma_t_sc_pc_pcb')
            drop_temp_table('tot_w_pc')
            # Get secondary category spend / moving average vs budget
            b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, person, primary)
            # Get primary category surplus and aggregate over secondary categories
            tot_w_pc(person)
        
            query_string = "SELECT * FROM b_w_scma_t_sc_pc_pcb;"
            #SC-level detail
            specific_sc_sql_tuple[person] = execute_query(query_string)
        
            query_string = "SELECT * FROM tot_w_pc;"
            #PC-level detail
            specific_pc_sql_tuple[person] = execute_query(query_string)

    viz_transaction_list = []

    if secondary is not None:
        # if secondary transaction granularity is needed, build table
        for index, row in enumerate(sc_sql_tuple): 
            if row.secondary_category == secondary:
                transaction = {}
                
                transaction['week_number'] = "Week " + str(row.week_number)
                transaction['primary_category'] = str(row.primary_category)
                transaction['secondary_category'] = str(row.secondary_category)
                transaction['week_spend'] = convert_to_float(row.sc_week_spend)
                transaction['moving_avg'] = convert_to_float(row.sc_moving_avg)
                
                # split by individual if necessary
                if individual is None:
                    transaction['ed_week_spend'] = convert_to_float(specific_sc_sql_tuple['ed'][index].sc_week_spend)
                    transaction['julie_week_spend'] = convert_to_float(specific_sc_sql_tuple['julie'][index].sc_week_spend)
                
                transaction['budgeted_amount'] = row.sc_budgeted_amount
                transaction['perc_remaining'] = round_viz(convert_to_float(row.sc_perc_remaining/100),1)
                transaction['perc_remaining_moving_avg'] = round_viz(convert_to_float(row.sc_perc_remaining_moving_avg/100),1)

                viz_transaction_list.append(transaction)
    else:
        sc_pivot_data = pivot_sc()

        # if secondary transaction granularity isn't needed, build table for PCs
        for index, row in enumerate(pc_sql_tuple): 
            if row.primary_category == primary:
                
                transaction = {}
                
                transaction['week_number'] = "Week " + str(row.week_number)
                transaction['primary_category'] = str(row.primary_category)
                transaction['week_spend'] = convert_to_float(row.pc_week_spend)
                transaction['moving_avg'] = convert_to_float(row.pc_moving_avg)
                
                # split by individual if necessary
                if individual is None:
                    transaction['ed_week_spend'] = convert_to_float(specific_pc_sql_tuple['ed'][index].pc_week_spend)
                    transaction['julie_week_spend'] = convert_to_float(specific_pc_sql_tuple['julie'][index].pc_week_spend)
                    
                transaction['budgeted_amount'] = row.pc_budgeted_amount
                transaction['perc_remaining'] = round_viz(convert_to_float(row.pc_perc_remaining/100),1)
                transaction['perc_remaining_moving_avg'] = round_viz(convert_to_float(row.pc_perc_remaining_moving_avg/100),1)
                
                for field in sc_pivot_data[index]._fields:
                    if field not in ['year_number','week_number','primary_category']:
                        transaction[field] = getattr(sc_pivot_data[index], field)
                
                
                viz_transaction_list.append(transaction)
                


    return viz_transaction_list

def round_viz(value, max):
    # Scale for visual purposes
    if abs(value) > max:
        if value < 0:
            value = -max
        else:
            value = max
    return value

def pivot_sc():
    #prerequisite: b_w_scma_t_sc_pc_pcb(startdate, enddate, moving_avg_weeks, person)
    
    query_string = """
        SET @sql = NULL;
        SET @@group_concat_max_len = 32000;
        """
    cursor_execute(query_string)
    
    query_string = """
        SELECT
          GROUP_CONCAT(DISTINCT
            CONCAT(
              'SUM(IF(secondary_category = ''',
              secondary_category,
              ''', sc_week_spend,0)) AS ',
              replace(replace(replace(secondary_category, ' ', ''), '&',''), '/','')
            )
          ) INTO @sql
        from b_w_scma_t_sc_pc_pcb;
        """
    cursor_execute(query_string)
    
    query_string = """
        SET @sql = CONCAT('SELECT year_number, week_number, primary_category,', @sql, ' FROM b_w_scma_t_sc_pc_pcb group by 1,2,3 order by primary_category, year_number, week_number');
        """
    cursor_execute(query_string)
    
    query_string = """
        PREPARE stmt FROM @sql;
        """
    cursor_execute(query_string)
    
    query_string = """
        EXECUTE stmt;
        """
        
    return execute_query(query_string) 