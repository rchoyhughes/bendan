//https://www.reddit.com/r/flask/comments/chm0qu/have_upvotedownvotes_buttons_that_dont_reload_the/

$(document).on('click', '#upvote-btn', function(event) {
    var $myurl=$('#upvote-btn').data('postid') + '/upvote';
    $.ajax({
    	url : $myurl,
    	type : "post",
    	contentType: 'application/json;charset=UTF-8',
    	dataType: "json",
    	data : JSON.stringify({'postid' : $('#upvote-btn').data('postid')}),
    	success : function(response) {
    		console.log(response);	
    	},
    	error : function(xhr) {
    		console.log(xhr);
    	}
    });
    event.preventDefault();
});