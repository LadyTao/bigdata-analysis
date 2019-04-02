import smtplib
import email

chst = email.charset.Charset(input_charset='utf-8')

# header里分别定义发件人,收件人以及邮件主题。
header = ("From: %s\nTo: %s\nSubject: %s\n\n" %
          ("m18098971185@163.com",
           "zs672087110@gmail.com",
           chst.header_encode("Mail Test")))

# 打开目标文档后读取并保存至msg这个多行str变量里。
#f = open("/usr/local/bigdata/logs/shelljob/ci_data/member_expire.log", 'r', encoding='utf-8')
f = open("insert_ci_order.sh", 'r', encoding='utf-8')
msg = ''' '''
while True:
    line = f.readline()
    msg += line.strip() + '\n'
    if not line:
        break
f.close()

# 对header和msg邮件正文进行utf-8编码，指定发信人的smtp服务器，并输入邮箱密码进行登录验证，最后发送邮件。
email_con = header.encode('utf-8') + msg.encode('utf-8')
smtp = smtplib.SMTP("smtp.163.com")
print("check the login in 163 email")
smtp.login("m18098971185@163.com", "zs18098971185")
#smtp.sendmail('m18098971185@163.com', '672087110@qq.com', email_con)
smtp.sendmail('m18098971185@163.com', 'zs672087110@gmail.com', email_con)
print("send email ok!!")
smtp.quit()