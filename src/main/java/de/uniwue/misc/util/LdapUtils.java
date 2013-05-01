package de.uniwue.misc.util;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * This class provides utils for ldap login. Users can reach two states:
 * authenticated and authorized.
 *
 * @author efzwo
 *
 */
public class LdapUtils {
	private static boolean authorized = false; // true: allows all operations,
												// false; allows viewing numbers
	private static boolean authenticated = false; // true: login checked and
													// confirmed
	private static String error = "";

	private static final String LDAP_BASE = "DC=ds,DC=efzwo,DC=de";
	private static final String LDAP_URL = "ldap://nas:389";

	/**
	 * try to login at configured ldap server with provided username and
	 * password
	 *
	 * @param user
	 *            the users username
	 * @param pw
	 *            the users password
	 */
	public static void login(String user, String pw) {
		try {
			String dn = "uid=" + user + ",cn=users," + LDAP_BASE;

			// Set up the environment for creating the initial context
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY,
					"com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.PROVIDER_URL, LDAP_URL);

			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, dn);

			// maybe this works for klinik ldap, does not work on my nas ds
			// env.put(Context.SECURITY_PRINCIPAL, "de\\efzwo\\ds\\" + user);

			env.put(Context.SECURITY_CREDENTIALS, pw);

			// Create the initial context
			DirContext ctx = new InitialDirContext(env);
			boolean result = ctx != null;

			if (ctx != null)
				ctx.close();

			authenticated = result;
			error = "";
		} catch (Exception e) {
			error = e.getMessage();
			authenticated = false;
		}
	}

	/**
	 * @return true if the user has logged in with confirmed name and pw, false
	 *         if not yet logged in or a login error occured
	 */
	public static boolean isAuthenticated() {
		return authenticated;
	}

	/**
	 * @return true if the user is allowed to see patient data details
	 */
	public static boolean isAuthorized() {
		return authorized;
	}

	/**
	 * @return error message from ldap login
	 */
	public static String getError() {
		return error;
	}
}
