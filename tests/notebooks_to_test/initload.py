# Databricks notebook source
### Override av felttype for noen kolonner under les fra Oracle. Vi vil ikke ha default kolonnetypen som databricks tolker for disse kolonnene.
order_schema = 'id string, status string, ordered_by_id string, contact_person_id string, collection_point_id string'
sales_trans_schema = 'original_id string, order_id string, status string, origin_country_id string, origin_lisa_id string, destination_country_id string, destination_lisa_id string, amount decimal(18,2), vat_amount decimal(18,2), vat_rate decimal(18,2), foreign_currency_amount decimal(18,2), quantity bigint, traveller_sequence_number bigint, is_foreign_vat boolean, transfer_to_id string, transfer_from_id string, relation_sequence_number bigint, sequence_number bigint, season_ticket_id string, ticket_group_id string, ticket_service string, service_to_date string, service_from_date string, transfer_to_price decimal(18,2), transfer_from_price decimal(18,2), transfer_to_season_ticket_id string, transfer_from_season_ticket_id string, passenger_category_id string, traveller_customer_number string, discount_id string, ten decimal(18,2), interconnection_code string'
payment_schema = 'order_id string, delbetaling_id string, betalingtransaksjon_id string, payment_agreement_original_id string, payment_method_id string, voucher_id string'
event_schema = 'order_id string,  point_of_sale_id string, shift_id string'
customer_schema = 'id string'


def INITLoadTogreise(dFrom, dTo):
    for d in range(dFrom, dTo):
        startdatetime = datetime.utcnow()
        df = oracle_query('SALG_DVH_DM', 'SELECT * FROM SALG_FAKTA_TOGREISE where FK_REISEDATO = {}'.format(d))
        df = df.add_metadata('LISADM')
        df.write_to_redshift('STAGE.LISADM_SALG_FAKTA_TOGREISE', mode="append")
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Date ' + str(d) + ' loaded in: ' + str(runtime) + '  (Start date {})'.format(startdatetime))

def INITLoadFlerreise(dFrom, dTo):
    startdatetime = datetime.utcnow()
    df = oracle_query('SALG_DVH_DM', 'SELECT * FROM SALG_FAKTA_FLERREISE where FK_REISEDATO BETWEEN {} and {}'.format(dFrom, dTo))
    df = df.add_metadata('LISADM')
    df.write_to_redshift('STAGE.LISADM_SALG_FAKTA_FLERREISE', mode="append")
    enddatetime = datetime.utcnow()
    runtime = enddatetime - startdatetime
    print('Date ' + str(dFrom) + ' to ' + str(dTo) + ' loaded in: ' + str(runtime) + '  (Start date {})'.format(startdatetime))

def INITLoadTjeneste(dFrom, dTo):
    for d in range(dFrom, dTo):
        startdatetime = datetime.utcnow()
        df = oracle_query('SALG_DVH_DM', 'SELECT * FROM SALG_FAKTA_TJENESTE where FK_REISEDATO = {}'.format(d))
        df = df.add_metadata('LISADM')
        df.write_to_redshift('STAGE.LISADM_SALG_FAKTA_TJENESTE', mode="append")
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Date ' + str(d) + ' loaded in: ' + str(runtime) + '  (Start date {})'.format(startdatetime))

### INIT last av V_ORDER fra lisa klone, basert på changed_at dato
def INIT_V_ORDER(start_date, end_date):
    while start_date <= end_date:
        start = start_date.strftime('%Y-%m-%d')
        start_date = start_date + timedelta(days=1)
        end = start_date.strftime('%Y-%m-%d')
        startdatetime = datetime.utcnow()
        df = oracle_query('LISA_PRODKLONE', """SELECT * FROM LISA.V_ORDER where changed_at >= TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') and changed_at < TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS')"""
                      .format(start, end), order_schema)
        df = df.add_metadata('LISA_PRODKLONE')
        df.upsert_to_redshift('STAGE.LISA_KLONE_INIT_V_ORDER', upsert_keys=['id'])
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Datespan '+ start + ' to ' + end + ' loaded in: ' + str(runtime))
    
### INIT load V_SALES_TRANSACTION fra lisa klone basert på changed_at date i V_ORDER(LISA.BESTILLING)
def INIT_V_SALES_TRANSACTION(start_date, end_date):  
    while start_date <= end_date:
        start = start_date.strftime('%Y-%m-%d')
        start_date = start_date + timedelta(days=1)
        end = start_date.strftime('%Y-%m-%d')
        startdatetime = datetime.utcnow()
        df = oracle_query('LISA_PRODKLONE', """select * from LISA.V_SALES_TRANSACTION where order_id in (SELECT LISA.BESTILLING.ID FROM LISA.BESTILLING \
                          where TIMESTAMP >= TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') and TIMESTAMP < TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') )"""
                          .format(start, end), sales_trans_schema)
        df = df.add_metadata('LISA_PRODKLONE')
        df.upsert_to_redshift('STAGE.LISA_KLONE_INIT_V_SALES_TRANSACTION', upsert_keys=['order_id'])
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Datespan '+ start + ' to ' + end + ' loaded in: ' + str(runtime))
        
### INIT load V_PAYMENT fra lisa klone basert på changed_at date i V_ORDER(LISA.BESTILLING)
def INIT_V_PAYMENT(start_date, end_date):  
    while start_date <= end_date:
        start = start_date.strftime('%Y-%m-%d')
        start_date = start_date + timedelta(days=1)
        end = start_date.strftime('%Y-%m-%d')
        startdatetime = datetime.utcnow()
        df = oracle_query('LISA_PRODKLONE', """select * from LISA.V_PAYMENT where order_id in (SELECT LISA.BESTILLING.ID FROM LISA.BESTILLING 
                          where TIMESTAMP >= TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') and TIMESTAMP < TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') )"""
                          .format(start, end), payment_schema)
        df = df.add_metadata('LISA_PRODKLONE')
        df.upsert_to_redshift('STAGE.LISA_KLONE_INIT_V_PAYMENT', upsert_keys=['order_id'])
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Datespan ' + start + ' to ' + end + ' loaded in: ' + str(runtime))
        
### INIT load V_ORDER_EVENT fra lisa klone basert på changed_at date i V_ORDER(LISA.BESTILLING)
def INIT_V_ORDER_EVENT(start_date, end_date):  
    while start_date <= end_date:
        start = start_date.strftime('%Y-%m-%d')
        start_date = start_date + timedelta(days=1)
        end = start_date.strftime('%Y-%m-%d')
        startdatetime = datetime.utcnow()
        df = oracle_query('LISA_PRODKLONE', """SELECT * FROM LISA.V_ORDER_EVENT where order_id in (SELECT LISA.BESTILLING.ID FROM LISA.BESTILLING 
                          where TIMESTAMP >= TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') and TIMESTAMP < TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') )"""
                          .format(start, end), event_schema)

        df = df.add_metadata('LISA_PRODKLONE')
        df.upsert_to_redshift('STAGE.LISA_KLONE_INIT_V_ORDER_EVENT', upsert_keys=['order_id'])
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Datespan ' + start + ' to ' + end + ' loaded in: ' + str(runtime))

### INIT load V_ORDER_CUSTOMER fra lisa klone basert på changed_at date i V_ORDER(LISA.BESTILLING)
def INIT_V_ORDER_CUSTOMER(start_date, end_date):  
    while start_date <= end_date:
        start = start_date.strftime('%Y-%m-%d')
        start_date = start_date + timedelta(days=1)
        end = start_date.strftime('%Y-%m-%d')
        startdatetime = datetime.utcnow()
        df = oracle_query('LISA_PRODKLONE', """SELECT * FROM LISA.V_ORDER_CUSTOMER where id in (SELECT LISA.BESTILLING.BESTILLEROBSERVASJON_ID FROM LISA.BESTILLING 
                          where TIMESTAMP >= TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') and TIMESTAMP < TO_DATE('{} 00:00:00','YYYY-MM-DD HH24:MI:SS') )""" 
                          .format(start, end), customer_schema)
        df = df.add_metadata('LISA_PRODKLONE')
        df.upsert_to_redshift('STAGE.LISA_KLONE_INIT_V_ORDER_CUSTOMER', upsert_keys=['id'])
        enddatetime = datetime.utcnow()
        runtime = enddatetime - startdatetime
        print('Datespan ' + start + ' to ' + end + ' loaded in: ' + str(runtime))
