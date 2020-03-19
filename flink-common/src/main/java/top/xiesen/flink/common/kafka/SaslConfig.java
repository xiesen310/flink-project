package top.xiesen.flink.common.kafka;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @className top.xiesen.flink.common.kafka.SaslConfig
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:50
 */
public class SaslConfig extends Configuration {
    private String username;
    private String password;

    public SaslConfig() {
    }

    public SaslConfig(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

        Map<String, String> options = new HashMap<String, String>(10);
        options.put("username", username);
        options.put("password", password);
        AppConfigurationEntry entry = new AppConfigurationEntry(
                "org.apache.kafka.common.security.plain.PlainLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        AppConfigurationEntry[] configurationEntries = new AppConfigurationEntry[1];
        configurationEntries[0] = entry;
        return configurationEntries;
    }
}