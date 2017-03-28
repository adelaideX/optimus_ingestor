import inspect
import os
import logging
import datetime
import config
import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate

basepath = os.path.dirname(__file__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
loggers = {}


def log(message):
    """
    Logs a message to screen and to the file
    :param message:
    """
    class_name = os.path.splitext(os.path.basename(inspect.stack()[1][1]))[0]
    if class_name not in loggers:
        log_file = basepath + '/logs/' + class_name + '.log'
        new_logger = logging.getLogger(class_name)
        new_log_handler = logging.FileHandler(log_file)
        new_logger.addHandler(new_log_handler)
        loggers[class_name] = new_logger
    the_log = loggers[class_name]
    the_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = the_time + ":" + " " + message
    the_log.info(log_message)


# get first child dir name of current
def get_subdir(a_dir):
    """
    Returns the path of the first child directory found in the main-path
    :param a_dir: The main path
    :rtype: string
    """
    for d in os.listdir(a_dir):
        p = os.path.join(a_dir, d)
        if os.path.isdir(p):
            return p


def send_mail(send_from, send_to, cc_to, subject, text, files=None,
              server="localhost"):
    assert isinstance(send_to, list)
    assert isinstance(cc_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['CC'] = COMMASPACE.join(cc_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)

    smtp = smtplib.SMTP(server)
    send_to += cc_to
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
