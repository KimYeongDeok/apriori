package org.openflamingo.hadoop.clean;

import org.openflamingo.hadoop.util.StringUtils;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class UserController {

	UserService userService;

	public void login(String username, String password) {

		if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
			throw new IllegalArgumentException("empty");
		}

		User user = userService.getUser(username);
		if (user == null) {
			throw new IllegalArgumentException("not exists");
		}

		if (!password.equals(user.password)) {
			throw new IllegalArgumentException("invalid passowrd");
		}

		// login success
	}

}
