import smtplib, ssl
import sys

port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "72ljennifer72@gmail.com"  # Enter your address
receiver_email = "72ljennifer72@gmail.com"  # Enter receiver address
password = "msifczxxhocfsmih"
failure_message = """\
Subject: FAILED TRIAL

Look at them!"""

success_message = """\
Subject: SUCCESSFUL TRIAL

You did it!"""


def email_failure():
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, failure_message)


def email_success():
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, success_message)


def main():
    email_failure()
    email_success()

    return 0


if __name__ == "__main__":
    sys.exit(main())
