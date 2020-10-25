# Databricks notebook source
dict_databricks_job = {}

# COMMAND ----------

marie_will_fix_it = """Please forward this email to Marie Hansteen and Matias Ferrero. But Marie will fix it ;) """  

# COMMAND ----------

dict_databricks_job['akp'] = marie_will_fix_it

# COMMAND ----------

dict_databricks_job['kundemaster_delta_load_v2'] = marie_will_fix_it 

# COMMAND ----------

dict_databricks_job['sales_live_entur'] = """Parametere

INGEN

Rekjøring

Ingen manuell re-kjøring.

Beskrivelse

sales_live_entur kjører parallellt med sales_live.
Henter data fra Enturs nye salgssystem, som gradvis vil overta for LISA.
Tar inn data fra Enturs kafka-strømmer, lagrer rådata i S3, stager data til staging-tabeller i Redshift og skriver til f_sales (Redshift).
Tips og Triks

Kjente feil:

Om Enturs Kafka-system har problemer, vil notebook stage_entur_kafka_to_S3 faile. Problemet kan være midlertidig, og notebooken
er skrevet slik at den vil "rekjøre" seg selv neste gang den kjøres. Ved gjentatte feilinger, kontakt teamet.
Skal kun varsle systemansvarlig dersom lasten feiler flere ganger på rad (eks. over 5 etterfølgende ganger)
"""

# COMMAND ----------

dict_databricks_job['sales_nightly_entur'] = """Parametere

arg_delta: settes til 0 hvis man ønsker å hente togavganger for 28 dager fra Kafka-kø.

Rekjøring

Rekjør kun om notebook stage_entur_kafka_raw_nightly har feilet.
Om notebook stage_entur_kafka_raw_nightly ikke har feilet, men en eller flere av stage_entur_sales-transactions, stage_entur_credits, stage_entur_payments har feilet, kjør jobben: sales_entur_hist_init_s3 (se nedenfor).
Andre notebooks er ikke kritiske, da feil vil rettes opp i automatisk ved neste kjøring.
Notebook stage/stage_kafka_entur_datedServiceJourneys besørger togavganger via Kafka-kø, og skriver til redshift→stage. Gjør daglige delta-ekstrakter.
Beskrivelse

Henter alle salg og togavganger som er kommet via Kafka de siste 24 timer, lagrer rådata i S3 og skriver til staging og f_sales i redshift. Om sales_live_entur har problemer vil sales_nightly_entur uansett rette opp i feil ved dagens utgang.

Tips og Triks

Kjente feil: Om Enturs Kafka-system har problemer, vil notebook stage.entur_kafka_raw_nightly feile. Om den feiler ved rekjøring også, kontakt teamet.
"""

# COMMAND ----------

dict_databricks_job['crm_export'] = """Parametere

INGEN

Rekjøring

Ingen manuell re-kjøring. Rekjøres av seg selv på timer.

Beskrivelse

crm_export er en live last som kjører mellom 07-23 hver dag.
Henter data fra LISA EXPORT API og skriver direkte til on-prem database (MSSQL).
Tips og Triks

Kjente feil:

Connection issues mot enturs LISA EXPORT API'et. Jobben vil vente 10 min før den prøver igjen.
Skal kun varsle systemansvarlig dersom lasten feiler flere ganger på rad (eks. over 10 etterfølgende ganger). I tillegg skal CRM (Per Kristian) informeres om at det er forsinkelse på salgsdata og at vi jobber med saken

"""

# COMMAND ----------

dict_databricks_job['ci_crm_customer_service'] = """Parametre	
arg_to og arg_from. Default er ikke-utfylte verdier, da jobben håndterer sin egen deltadeteksjon. Arg_init settes til '1' for initiell last.

Format: 'yyyy-mm-dd'



Rekjøring	Når som helst
Beskrivelse	
jobben henter data fra CRM-Replica, skriver til Redshift→Stage, for videre lasting av Public.ci_crm_cases, Public.ci_crm_categorization og

Public.ci_crm_survey
Område er kundetilfredshet, tilbakemeldinger fra kunder.

 Tips og Triks	 """

# COMMAND ----------

dict_databricks_job['vy_app'] = """Parametre	
arg_to og arg_from. Default er ikke-utfylte verdier, da jobben håndterer sin egen deltadeteksjon.

Format: 'yyyy-mm-dd'



arg_init settes til 1 hvis man ønsker full historikk for alle kunder som har en eller annen endring i tidsrommet jobben kjører for. Default er tom verdi.

Rekjøring	Når som helst
Beskrivelse	Jobben ekstraherer 'Favorites' og 'Installations' fra App
 Tips og Triks	 """


# COMMAND ----------

dict_databricks_job['crm_qa'] = """Parametere

INGEN

Rekjøring

Kjøres hver andre time, trenger ikke rekjøre manuelt hvis den feiler.

DbFit test må verifiseres (se rutine for DbFit tester)

Beskrivelse

Dette er en jobb som kun kjører DbFit tester (datakvalitet) på crm export dataene.

Tips og Triks

Kjente feil

Feiler automatisk hvis crm_export feiler"""

# COMMAND ----------

dict_databricks_job['sales_live'] =""" Parametere

INGEN

Rekjøring

Ingen manuell re-kjøring. Rekjøres av seg selv på timer.

Beskrivelse

sales_live er en live last som kjører mellom 07-23 hver dag.
Henter data fra LISA EXPORT API, transformerer og skriver til redshift.
Tips og Triks

Kjente feil:

Connection issues mot ENTUR LISA EXPORT API'et. Jobben vil vente 10 min før den prøver igjen.
Skal kun varsle systemansvarlig dersom lasten feiler flere ganger på rad (eks. over 10 etterfølgende ganger)
  """

# COMMAND ----------

dict_databricks_job['sales_dimensions'] = """ Parametere

INGEN

Rekjøring

Rekjøres om den feiler.
Lag Jira sak og informer systemansvarlig om at jobben har feilet hvis den fortsetter å feile.
Beskrivelse

Stager data fra mange forskjellige kilder.

Tips og Triks

Kjente feil:"""

# COMMAND ----------

dict_databricks_job['sales_weekly'] = """Parametere

INGEN

Rekjøring

Klikk deg inn på jobben og se om det er noe DbFit relatert. Hvis ikke DbFit. Prøv å re-kjøre jobben.
Dersom den fortsatt feiler, send e-post til systemansvarlig med info (med dvh vakt på cc), legg inn sak i JIRA og assign til systemansvarlig
Beskrivelse

Stager data fra mange forskjellige kilder som bare trengs å kjøre ukentlig, samt relaster lengere tilbake i tid enn sales_nighly.

Tips og Triks

"""

# COMMAND ----------

dict_databricks_job['sales_pakketurer'] = """Parametere

INGEN

Rekjøring

Rekjøres hvis den feiler. Jobben gjør en full last av alle data fra EOS inn i f_sales.

Beskrivelse

Jobben kjøres hver dag.
Henter data om pakketurer fra EOS og skriver til f_sales. Gjør en full last av alle data for hver kjøring, ingen deltalogikk.
Tips og Triks

"""

# COMMAND ----------

dict_databricks_job['sales_tester'] = """Parametere

INGEN

Rekjøring

Rekjøres IKKE.

Beskrivelse

Følg rutine i henhold til dbFit tester ved feil. (link)
Jobben kjøres hver dag.
Kjører dbFit tester + aggregeringer som lagres som kan brukes for feilsøking på historiske endringer av data.
Tips og Triks

"""

# COMMAND ----------

dict_databricks_job['total_rides'] = """Parametere

INGEN

Rekjøring

Rekjøres hvis den feiler. Jobben gjør en full last et år tilbake i tid hver dag.

Beskrivelse

Laster flere kilder som resulterer i tabellen "f_total_rides". Tella, manuelle input og f_sales brukes til å generere tabellen.

Tips og Triks

Hvis jobben feiler og det er den 6. virkedag, undersøk med Ida om hun allerede har tatt ut tallene fra snapshot for dagen.

Hvis ja, ikke behov for re-kjøring av jobben før dagen etter
Hvis nei, re-kjør jobben og qlik sense modellen "totalt antall reiser" før du informer Ida om at jobben har kjørt ferdig""" 

# COMMAND ----------

dict_databricks_job['sales_tkab_init'] = """Parametere

Rekjøring

Kan rekjøres om den feiler og ikke har kjørt med suksess siden.

Beskrivelse

Kjøres én gang hver dag.

Laster inn data fra kildene SilverRail og OpenSolution og sammenstiller de til f_sales_tkab.

Tips og Triks

Det hender jobben feiler pga avbrutt tilkobling mot APIet til OpenSolution, men da kan jobben bare startes på nytt. Hvis jobben feiler igejn er det sannsynligvis en feil i koden og da må man sette igang feilsøking."""

# COMMAND ----------

dict_databricks_job['sales_tkab_weekly'] = """ Parametere


Rekjøring

Behøver ikke rekjøres om den feiler og om sales_tkab_nightly har kjørt siden.

Beskrivelse

Jobben gjør en fullast på OpenSolution-data hver lørdag. Kjører ikke staging av SilverRail-data.

Tips og Triks

Det hender jobben feiler pga avbrutt tilkobling mot APIet til OpenSolution, men da kan jobben bare startes på nytt. Hvis jobben feiler igejn er det sannsynligvis en feil i koden og da må man sette igang feilsøking."""

# COMMAND ----------

dict_databricks_job['green_mobility_bybil_financesales_of'] = """Parametere

INGEN

Rekjøring

Se egen dokumentasjon for denne.

Beskrivelse



Tips og Triks

Dokumentasjon: Green Mobility - Bybil"""

# COMMAND ----------

dict_databricks_job['export_prm_to_entur'] = """Parametere

INGEN

Rekjøring

Rekjøres hvis den feiler.

Informer Imre Kerr at jobben har feilet og at den har blitt rekjørt

Beskrivelse

Henter data fra LISA som PRM bruker for å sende til Entur.

Tips og Triks

"""

# COMMAND ----------

dict_databricks_job['prm_daily'] = """Parametere

INGEN

Rekjøring

Rekjøres hvis den feiler.

Informer Imre Kerr at jobben har feilet og at den har blitt rekjørt

Beskrivelse

Henter data fra KK tabeller + views på LISA PROD. Skriver til S3 bucket som PRM leser fra.

Tips og Triks

Det er Imre Kerr som har laget jobben, ikke direkte en del av KK/SalgDVH."""

# COMMAND ----------

dict_databricks_job['meteorologisk_institutt_oracle'] = """Parametere

arg_init, settes til '1' hvis man ønsker å hente værdata for spesifikke fra- og tildatoer

arg_from: fradato, eks på format: 2019-05-17

arg_to: tildato, eks på format: 2019-05-17

Rekjøring

Rekjøres hvis den feiler.



Beskrivelse

Jobben ekstraherer værstasjoner, samt værobservasjoner som temperatur, snødybde, vind og vindretning fra Meteorologisk Institutt.

Disse skrives direkte til onprem oracle stage, for videre konsummering I Datavault. Sistnevnte ligger I wf_Journey som kjøres daglig.

Tips og Triks

Det er grenser for hvor mange værobservasjoner vi kan hente ved hver kall mot API-et. 4 dager for 121 stasjoner ser ut til å fungere bra.

Vær oppmerksom på at dette kan være en feilårsak, og da må intervallet skrus på I notebook export/meteorlogisk_institutt_api_oracle_admstg."""

# COMMAND ----------

dict_databricks_job['sales_nightly_turnit_parallel'] = """Parametere

arg_dag (default = 'nightly'), arg_from ('yyyy-mm-dd', default = '2018-12-01'), arg_to ('yyyy-mm-dd'), arg_init (default = 0)

Rekjøring

Ingen re-kjøring per nå.
Dersom feiling av jobb, skyldes feil ved lesing av data enten fra Turnit sin database eller historiske data i S3.
Informer Marie, Sindre eller Jan om at jobben har feilet
Beskrivelse

Stager og kjører last til faktatabeller fra Turnit sine database views og historiske data i S3, som lagres henholdsvis i Redshift-mappene "stage" og "public".

Tips og Triks

Kjente feil:

Inntil videre vil denne jobben vises som feilet grunnet lesefeil til kilde."""

# COMMAND ----------

dict_databricks_job['kundemaster_abt_akp_customer_foundation'] = """Parametere

ingen

Rekjøring

Ingen re-kjøring per nå., da jobben kjører hver fjerde time. Hvis den feiler hyppigere, må man undersøke nærmere.

Beskrivelse

Jobben leser kunder fra SQS. Dette er en meldingskø driftet av Bekk/CRM. Jobben er skedulert til å kjøre hver fjerde time, der man ekstraherer nye og endrede kunder.

Tips og Triks

Kjente feil:

Jobben ser ut til å feile én gang om dagen, uvisst av hvilken grunn."""

# COMMAND ----------

dict_databricks_job['customerinsight_clv'] = """Parametere

Skriptet som kalles fra DAG har parameter som er satt til 1 i Job med samme navn.

Denne har nå oppføring for load_customerinsight_clv_train

Rekjøring

Kan rekjøres uten videre.

Beskrivelse

Jobben leser fra Redshift public.abt_akp_aggr_sales_customer, og skriver til Redshift Public.ci_clv_train.

Den leverer 'Customer Lifetime Value' eller CLV til Sense ifm. Kundeinnsikt.

Tips og Triks

Ingen."""

# COMMAND ----------

dict_databricks_job['personalbillett_entur_vy'] = """Parametere

Skriptet som kalles fra DAG har parameter som er satt til 1 i Job med samme navn.



{   "poststed": "1",  

 "familiemedlemmer": "1",

   "rettighetshaverimport": "1",

  "billettyper": "1",

  "billettrettighet": "1",  

 "billettinnehaver": "1",  

 "annual_load_all_tables": "1",

   "rettighetshaver": "1",

  "tjenestested": "1" }



annual_load_all_tables settes til 1 hvis man ønsker å kjøre jobb for alle tabeller, samtidig som de øvrige parametre er satt til 1. Normalt vil annual_load_all_tables være satt til 0, da den bare skal kjøre manuelt én gang i året.

Hvis man ønsker å rekjøre tabeller enkeltvis, eksempelvis hvis en tabell feilet mens de øvrige gikk ok, setter man parametre til 0.

Når annual_load_all_tables er satt til 0, vil tjenestested og rettighetshaverimport automatisk kjøre uansett parameterinnstillinger. Dette vil være modusen for de daglige kjøringer."""

# COMMAND ----------

dict_databricks_job['ci_customer_consolidation'] = """Parametere

arg_dag (default = 'nightly'), arg_init (default = 0), arg_name_date (default = 1)

Rekjøring

Kan rekjøres.
Informer Marie dersom jobben har feilet
Beskrivelse

Jobben står for last av kundedata til konsolideringsprosjektet



Tips og Triks

Kjente feil: Dersom feiling av jobb, skyldes feil ved lesing av data fra AKP."""

# COMMAND ----------

dict_databricks_job['ci_crm_marked_parallel_nbpool'] = """Parametere

arg_dag (default = 'nightly'), arg_from ('yyyy-mm-dd', default = '2018-12-01'), arg_to ('yyyy-mm-dd'), arg_init (default = 0)

Rekjøring

Ingen re-kjøring per nå.
Dersom feiling av jobb, skyldes feil ved lesing av data fra crm sin replika database.
Informer Marie dersom jobben har feilet
Beskrivelse

Stager og kjører last til faktatabeller fra crm sin replika database, som lagres henholdsvis i Redshift-mappene "stage" og "public". Deretter kjører den

load_f_ci_on_boarding_total_percentage_PF_overview som lagrer en aggregert tabell for å følge utviklingen på den prosentvise profilfyllingsgraden.

Tips og Triks

Kjente feil:

Inntil videre vil denne jobben vises som feilet grunnet lesefeil til kilde."""
