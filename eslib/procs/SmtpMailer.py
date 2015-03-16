__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
import smtplib, getpass, platform
from email.mime.text import MIMEText
from eslib.esdoc import tojson


class SmtpMailer(Processor):
    """
    Send incoming document as content to recipients.
    Sends mail outgoing on port 25 unless a username/password is specified, in which case
    it uses TLS on port 587.
    Sender defaults to current executing user if not specified.

    Connectors:
        input      (*)       : Incoming documents to send. Non-string documents are converted to JSON.

    Config:
        smtp_server       = "localhost"
        username          = None
        password          = None
        sender            = None
        from_name         = None          : Name to be added to sender into the From field, becomes: '"from_name" <user@domain.com>'
        recipients        = []            : List of recipient email addresses (no mail or brackets or other fuzz).
        subject           = None
    """
    def __init__(self, **kwargs):
        super(SmtpMailer, self).__init__(**kwargs)

        self.create_connector(self._incoming, "input", "str", "Email content string.")

        self.config.set_default(
            smtp_server         = "localhost",
            username            = None,
            password            = None,
            sender              = None,
            from_name           = None,
            recipients          = None,
            subject             = None,
        )

    def on_open(self):
        self.count = 0

    def _incoming(self, doc):
        if not doc or not self.config.recipients or not self.config.sender:
            return

        # Convert non-string documents to JSON
        content = doc
        if not isinstance(doc, basestring):
            content = tojson(doc)

        try:
            self._mail_text(
                self.config.smtp_server,
                self.config.recipients,
                self.config.subject,
                self.config.sender,
                self.config.from_name,
                content,
                self.config.username,
                self.config.password)
            self.count += 1
        except Exception as e:
            self.log.exception("Failed to send email.")


    def _mail_text(self, smtp_server, recipients, subject, sender=None, from_name=None, content=None, username=None, password=None):
        msg = MIMEText(content, "plain", "utf-8")

        if not sender:
            sender = "@".join((getpass.getuser(), platform.node()))

        message_from = sender if not from_name else '"%s" <%s>' % (from_name, sender)

        msg['Subject'] = subject
        msg['From']    = message_from
        msg['To']      = ", ".join(recipients)

        s = None
        if username or password:
            s = smtplib.SMTP(smtp_server, 587)
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(username, password)
        else:
            s = smtplib.SMTP(smtp_server or "localhost")

        s.sendmail(sender, recipients, msg.as_string())
        s.quit()

