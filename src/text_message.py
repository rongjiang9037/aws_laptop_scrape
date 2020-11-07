from twilio.rest import Client
  
# Your Account SID from twilio.com/console
account_sid = "ACde61f88f78c4d03db0afa78ba187082a"
# Your Auth Token from twilio.com/console
auth_token  = "5d21421134ababd20b3676f0ca2b2da2"

client = Client(account_sid, auth_token)

def send_msg(msg):
    message = client.messages.create(
        to="+15512275432",
        from_="+12186667122",
        body=msg)
                   
