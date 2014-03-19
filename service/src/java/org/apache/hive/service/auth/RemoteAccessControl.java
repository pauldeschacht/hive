package org.apache.hive.service.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.conf.HiveConf;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;


import org.glassfish.jersey.client.ClientConfig;

public class RemoteAccessControl {

    public static final Log LOG = LogFactory.getLog(RemoteAccessControl.class.getName());

    private boolean bIgnore = false;
    
    private Client client = null;
    private WebTarget webTarget = null;

    public RemoteAccessControl(HiveConf conf) {
        String aclRestUri = System.getenv("MULTITENANT_ACL_URI");
        if (aclRestUri==null) {
            aclRestUri=conf.get("multitenant.acl.uri");
        }

        LOG.info("RemoteAccessControl uri = " + aclRestUri);
        if ("ignore".equals(aclRestUri) == true) {
            bIgnore = true;
        }
        ClientConfig clientConfig = new ClientConfig();
        client = ClientBuilder.newClient(clientConfig);
        webTarget = client.target(aclRestUri);
    }

    public boolean hasAccess(String user, String sql) {
        LOG.info("ACL for user [" + user + "] and statement [" + sql + "]");
        if(bIgnore==true) {
            return true;
        }
        if(user==null) {
            return false;
        }
        if(sql==null) {
            return true;
        }
        try {
            Response response = webTarget.queryParam("user",user).queryParam("sql",sql).request(MediaType.APPLICATION_JSON).get();
            if(response.getStatus() == 200) {
                return true;
            }
            else {
                return false;
            }
        }
        catch (Exception e) {
            LOG.error(e.toString());
            return false;
        }
    }
}
