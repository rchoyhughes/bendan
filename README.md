# bendan
## by Rob Hughes and Ben Jiang for ECE 49595 ##

### API Documentation: ###

**/getTSVdump**, {GET, POST}

Renders the TSV download page with two buttons for the TSV database download of the users table and posts table.

**/getTSVfile/<tbtype>**, {GET}

Processes and creates a TSV file of the requested table. Returns the file and triggers the file download.

**/upvote**, {POST}

Processes the upvote request submitted by the user pressing the upvote button on a post. Checks whether the user has upvoted a post and change status accordingly.

**/downvote**, {POST}

Processes the downvote request submitted by the user pressing the downvote button on a post. Checks whether the user has downvoted a post and change status accordingly.

**/<private_id>/feed**, {GET, POST}

Redirects user to the first page of their feed.

**/<private_id>/feed/<page>**, {GET, POST}

Redirects user to some page of their feed. Each page contains 5 posts, from most recent to least recent.

**/post/<post_id>/delete**, {POST}

Deletes the chosen post from the central database of posts.

**/<private_id>/create-submit**, {POST}

Takes the submitted data from the “Create a Post” form and inserts it into the central database of posts.

**/<private_id>/create**, {GET, POST}

Takes users to the “Create a Post” form.

**/<private_id>/profile**, {GET, POST}

Verifies that the current user is both authenticated and that the user ID exists, then loads the profile page of the user corresponding to the private_ID. The user’s previously posted posts are loaded.

**/post/<post_id>**, {GET, POST}

Loads the post corresponding to the post_ID.

**/login-submit**, {POST}

Verifies that the user has entered a valid username, and then submits the user’s login information to the central user database, if the username doesn’t already exist. If the username already exists, but the private ID doesn’t match, the user has entered an incorrect password and is prompted to try again. If the username exists and the private ID matches, the user is authenticated and taken to their feed. If the user has uploaded a profile picture, the file format is checked to see if it is a ‘.png’ or ‘.jpg’, and then it is stored locally in ‘/static/img/profile_pics’.

**/login, /register, /**, {GET, POST}

If there is a user already signed in, going to this page will sign them out. The login screen is then loaded.
