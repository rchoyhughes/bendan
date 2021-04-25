$(document).on('click', '#upvote-btn', function(event) {
    $.ajax({
    	url : $('#upvote-btn').data('postid') + '/upvote',
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