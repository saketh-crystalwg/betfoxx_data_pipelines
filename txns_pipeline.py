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

end = datetime.today() - timedelta(0)

start = datetime.today() - timedelta(1)

end_day = datetime(end.year, end.month, end.day, 0, 0, 0)

start_day = datetime(start.year, start.month, start.day, 0, 0, 0)

# Format the datetime object into the desired string format

end_time = end_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')

start_time = start_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')


txn_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=0&LanguageId=en'

txn_data = {"Controller":"PaymentSystem",
            "Method":"GetPaymentRequestsPaging",
            "RequestObject":{
                "Controller":"PaymentSystem",
                "Method":"GetPaymentRequestsPaging",
                "SkipCount":0,
                "TakeCount":9999,
                "OrderBy":None,
                "FieldNameToOrderBy":"",
                "Type":2,
                "HasNote":False,
                "FromDate":start_time,"ToDate":end_time},
            "UserId":"1780","ApiKey":"betfoxx_api_key"}

txn_response = requests.post(txn_url, json=txn_data)

txn_response_data = txn_response.json()

txn_entities = txn_response_data['ResponseObject']['PaymentRequests']['Entities']

txns = pd.DataFrame(txn_entities)

txn_date = datetime.today() - timedelta(1)

txn_date_1  = txn_date.strftime('%Y-%m-%d')

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

txns['Payment_Method'] = ['InternationalPSP' if x == 326 \
                                       else 'NOWPay' if x == 147 \
                                       else 'XcoinsPayCard' if x == 324 \
                                       else 'XcoinsPayCrypto' if x == 323 \
                                       else 'Omer' if x == 345 \
                                       else 'PayOpPIX' if x == 160 \
                                       else 'PayOpNeosurf' if x == 159 \
                                       else 'PayOpNeosurfUK' if x == 347 \
                                       else 'PayOpBankAT' if x == 352 \
                                       else 'PayOpRevolut' if x == 161 \
                                       else 'PayOPInterac' if x == 348 \
                                       else 'PayOpCashToCode' if x == 350 \
                                       else 'PayOpRevolutUK' if x == 356 \
                                       else 'PayOpBankUK' if x == 353 \
                                       else 'PayOpMonzo' if x == 349 \
                                       else 'Others' for x in txns['PaymentSystemId']]

wthdrl_data = {
  "Controller": "PaymentSystem",
  "Method": "GetPaymentRequestsPaging",
  "RequestObject": {
    "Controller": "PaymentSystem",
    "Method": "GetPaymentRequestsPaging",
    "SkipCount": 0,
    "TakeCount": 9999,
    "OrderBy": 0,
    "FieldNameToOrderBy": "ClientId",
    "Type": 1,
    "HasNote": False,
    "FromDate": start_time,
    "ToDate": end_time
  },
  "UserId":"1780","ApiKey":"betfoxx_api_key"}
  
wthdrl_response = requests.post(txn_url, json=wthdrl_data)

wthdrl_response_data = wthdrl_response.json()

wthdrl_entities = wthdrl_response_data['ResponseObject']['PaymentRequests']['Entities']

wthdrls = pd.DataFrame(wthdrl_entities)

wthdrls['Status'] = ['Approved' if x == 8 \
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
                  else 'NA' for x in wthdrls['State']]

wthdrls['Payment_Method'] = ['InternationalPSP' if x == 326 \
                                       else 'NOWPay' if x == 147 \
                                       else 'XcoinsPayCard' if x == 324 \
                                       else 'XcoinsPayCrypto' if x == 323 \
                                       else 'Omer' if x == 345 \
                                       else 'PayOpPIX' if x == 160 \
                                       else 'PayOpNeosurf' if x == 159 \
                                       else 'PayOpNeosurfUK' if x == 347 \
                                       else 'PayOpBankAT' if x == 352 \
                                       else 'PayOpRevolut' if x == 161 \
                                       else 'PayOPInterac' if x == 348 \
                                       else 'PayOpCashToCode' if x == 350 \
                                       else 'PayOpRevolutUK' if x == 356 \
                                       else 'PayOpBankUK' if x == 353 \
                                       else 'PayOpMonzo' if x == 349 \
                                       else 'Others' for x in wthdrls['PaymentSystemId']]
  
                                       
## Game Summaries
gs_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=0&LanguageId=en'

gs_data = {
  "Controller": "Dashboard",
  "Method": "GetBetsInfo",
  "RequestObject": {
    "Controller": "Dashboard",
    "Method": "GetBetsInfo",
    "Loading": False,
    "FromDate": start_time,
    "ToDate": end_time
  },
  "UserId": "1780",
  "ApiKey": "betfoxx_api_key",
  "Loading": False
}

gs_response = requests.post(gs_url, json=gs_data)

gs_response_data = gs_response.json()

gs_entities = gs_response_data['ResponseObject']['DailyInfo']

game_summaries = pd.DataFrame(gs_entities)

game_summaries['Date'] = pd.to_datetime(game_summaries['Date'], errors='coerce')

game_summaries['Date'] = game_summaries['Date'].dt.date

## Customers

cust_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=0&LanguageId=en'

cust_data = {
    "Controller": "Client",
    "Method": "GetClients",
    "RequestObject": {
        "Controller": "Client",
        "Method": "GetClients",
        "SkipCount": 0,
        "TakeCount": 9999,
        "OrderBy": None,
        "FieldNameToOrderBy": "",
        "CreatedFrom": start_time,
        "CreatedBefore": end_time
    },
    "UserId": "1780",
    "ApiKey": "betfoxx_api_key"
}

cust_response = requests.post(cust_url, json=cust_data)

cust_response_data = cust_response.json()

cust_entities = cust_response_data['ResponseObject']['Entities']

customers = pd.DataFrame(cust_entities)

try:
    engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)
    txns.to_sql('customer_transactions_betfoxx', con = engine, if_exists='append')
    
    wthdrls.to_sql('customer_transactions_betfoxx', con = engine, if_exists='append')
    
    game_summaries.to_sql('game_summaries_day_level', con = engine, if_exists='append')
    
    customers.to_sql('customers_betfoxx', con = engine, if_exists='append')
    
    subject = f'Betfoxx data ingestion for {txn_date_1} is Successful'
    
    body = f"Betfoxx data ingestion for {txn_date_1} is Successful\n"
    
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)
except Exception as ex:
    subject = f'Betfoxx Transactions data ingestion for {txn_date_1} is Failed'
    body = f"Betfoxx Transactions data ingestion for {txn_date_1} is failed due to \n {str(ex)}"
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)