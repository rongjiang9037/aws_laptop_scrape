from twilio.rest import Client
  
# Your Account SID from twilio.com/console
account_sid = ""
# Your Auth Token from twilio.com/console
auth_token  = ""

client = Client(account_sid, auth_token)

def send_msg(msg):
    message = client.messages.create(
        to="+15512275432",
        from_="+12186667122",
        body=msg)
                   
