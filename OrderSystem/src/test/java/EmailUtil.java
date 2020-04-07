import org.apache.commons.mail.EmailException;

import org.apache.commons.mail.HtmlEmail;


/**
 * 邮件发送工具类
 *
 * @author 王俊
 */
public class EmailUtil {
    private static String mailHost = "smtp.qq.com";
    private static String mailEncoding = "utf-8";
    private static String mailTo = "1559924775@qq.com";
    private static String mailSmtpPort = "587";
    private static String mailFrom = "1559924775@qq.com";
    private static String mailNickName = "wangjun";
    private static String mailUserName = "1559924775@qq.com";
    private static String mailPassword = "mrqbtnvqwwbsbabh"; //用这个开启POP3/SMTP服务时的密码

    public static void sendEmail(String subject, String message) {
        //发送email
        HtmlEmail email = new HtmlEmail();
        try {
            //这里是SMTP发送服务器的名字：qq的如下："smtp.qq.com"
            email.setHostName(mailHost);
            // 字符编码集的设置
            email.setCharset(mailEncoding);
            //收件人的邮箱
            email.addTo(mailTo);
            //设置是否加密
            email.setSSLOnConnect(false);
            //设置smtp端口
            email.setSmtpPort(Integer.parseInt(mailSmtpPort));
            // 发送人的邮箱
            email.setFrom(mailFrom, mailNickName);
            // 如果需要认证信息的话，设置认证：用户名-密码。分别为发件人在邮件服务器上的注册名称和授权码
            email.setAuthentication(mailUserName, mailPassword);
            // 要发送的邮件主题
            email.setSubject(subject);
            // 要发送的信息，由于使用了HtmlEmail，可以在邮件内容中使用HTML标签
            email.setMsg(message);
            // 发送
            email.send();
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        sendEmail("nihao", "<p>efeaaafe</p>");
    }

}
