# Databricks notebook source
### Konverter timestamp fra UTC til Oslo tid. Men fjerner timezone info.
def utc_to_oslo(timestmap):
  from_zone = tz.gettz('UTC')
  to_zone = tz.gettz('Europe/Oslo')
  
  timestmap = timestmap.replace(tzinfo=from_zone)
  timestmap = timestmap.astimezone(to_zone)
  
  return timestmap.replace(tzinfo=None)

# COMMAND ----------

### Nyttefunksjon som printer med timestamp.
### Kan også kalles med newline=False for å gi mulighet til senere å skrive mer til samme linje.
def logprint(text, newline=True):
  import datetime
  output = "[{:%Y-%m-%d %H:%M:%S}] {}".format(utc_to_oslo(datetime.datetime.now()), text)

  if newline: print(output)
  else: sys.stdout.write(output)
lp = logprint    

# COMMAND ----------

### Nyttefunksjon som kan time hvor lang tid forskjellige operasjoner tar.
### Du starter timeren med å initiere objektet, og stanser og henter resultatet med stop()
### Resultatet returneres som en string: 00:00:00
class Timer():
  
  ### from module datetime import class Datetime  
  
  def __init__(self):
    from datetime import datetime as Datetime
    self.start_timestamp = Datetime.now()
  
  def stop(self):
    
    from datetime import datetime as Datetime
    delta = (Datetime.now() - self.start_timestamp).seconds
    hours, remainder = divmod(delta, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)

# COMMAND ----------

### Sender en melding til en Slack-kanal.
### Se https://api.slack.com/docs/message-formatting for formatteringstips.
def send_slack(message=None, attachments=None, channel=None, verbose=True):
  if not channel: channel = env("SLACK_CHANNEL")
  from slacker import Slacker
  Slacker(env("SLACK_TOKEN")).chat.post_message(
    channel,
    message,
    username=env("SLACK_USER"),
    icon_emoji=':tractor:',
    attachments=attachments
  )
  if verbose: logprint("Melding sendt til slack")

# COMMAND ----------

### Setter starttidspunktet til når denne setup-filen kjøres. Ikke helt 100% men bør være helt fint for dette formålet.
NOTEBOOK_START_DATETIME = datetime.now()

### Skriver en metadatalogg til Redshift
def write_metadata_record(notebook, arg_from=None, arg_to=None):
  cols = {}
  cols['notebook'] = notebook
  cols['etl_insert'] = (datetime.now() + timedelta(hours=1))
  
  s = (datetime.now() - NOTEBOOK_START_DATETIME).total_seconds()
  cols['time_to_stage'] = '{:02.0f}:{:02.0f}:{:02.0f}'.format(s // 3600, s % 3600 // 60, s % 60)
  
  if arg_from: cols['arg_from'] = arg_from
  if arg_to:   cols['arg_to']   = arg_to
    
  print("INSERT INTO util.load_orders_meta ({}) VALUES ({});".format(
    ', '.join(cols.keys()),
    ', '.join(["'{}'".format(v) for k,v in cols.items()])
  ))
