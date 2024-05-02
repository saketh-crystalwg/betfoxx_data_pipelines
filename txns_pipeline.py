import json
import pandas as pd
import  requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import datetime as dt
from datetime import datetime, timedelta

def send_mail(send_from,send_to,subject,text,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

sender = "sakethg250@gmail.com"
recipients = ["sakethg250@gmail.com","isaac@crystalwg.com"]
password = "xjyb jsdl buri ylqr"

# Get yesterday's date

date_1 = datetime.today() - timedelta(0)

end_day = datetime(date_1.year, date_1.month, date_1.day, 0, 0, 0)

# Format the datetime object into the desired string format

end_time = end_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')

txn_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=4&LanguageId=en'

txn_data = {"Controller":"PaymentSystem",
            "Method":"GetPaymentRequestsPaging",
            "RequestObject":{
                "Controller":"PaymentSystem",
                "Method":"GetPaymentRequestsPaging",
                "SkipCount":0,
                "TakeCount":1000,
                "OrderBy":None,
                "FieldNameToOrderBy":"",
                "Type":2,
                "HasNote":False,
                "FromDate":"2024-01-01T00:00:00.000Z","ToDate":end_time},
            "UserId":"1780","ApiKey":"betfoxx_api_key"}

txn_response = requests.post(txn_url, json=txn_data)

txn_response_data = txn_response.json()

txn_entities = txn_response_data['ResponseObject']['PaymentRequests']['Entities']

txns = pd.DataFrame(txn_entities)

txns['Status'] = ['Approved' if x == 8 \
                  else 'ApprovedManually' if x == 12 \
                  else 'Cancelled' if x == 2 \
                  else 'CancelPending' if x == 14 \
                  else 'Confirmed' if x == 7 \
                  else 'Declined' if x == 6 \
                  else 'Deleted' if x == 11 \
                  else 'Expired' if x == 13 \
                  else 'Failed' if x == 9 \
                  else 'Frozen' if x == 4 \
                  else 'InProcess' if x == 3 \
                  else 'Pay Pending' if x == 10 \
                  else 'Pending' if x == 1 \
                  else 'Splitted' if x == 15 \
                  else 'Waiting For KYC' if x == 5 \
                  else 'NA' for x in txns['State']]

row_count = txns.shape[0]

try:
    engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)
    txns.to_sql('customer_transactions_betfoxx', con = engine, if_exists='replace')
    
    subject = f'Betfoxx Transactions data ingestion for {txn_date_1} is Successful'
    body = f"Betfoxx Transactions data ingestion for {txn_date_1} is Successful and have ingested {row_count} records"
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)
except Exception as ex:
    subject = f'Betfoxx Transactions data ingestion for {txn_date_1} is Failed'
    body = f"Betfoxx Transactions data ingestion for {txn_date_1} is failed due to \n {str(ex)}"
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)