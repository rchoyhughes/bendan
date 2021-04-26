

$(document).ready(function(){
	$('.upvote').click(function() {
		console.log('Ajax called');
		console.log($(this).data('postid'));
		const postID = $(this).data('postid');
		$.ajax({
			url : '/upvote',
			type : 'POST',
			contentType: 'application/json;charset=UTF-8',
			dataType: "json",
			data : JSON.stringify({'postid' : $(this).data('postid')}),
			success : function(response) {
				console.log(response);
				if (response.status === 'success'){
					console.log("Got that update!!!!!!!!!!");
					document.getElementById(postID).innerHTML ='&nbsp;<strong>' + response.upvotes.toString() + '</strong>&nbsp;';
				}

			},
			error : function(xhr) {
				console.log(xhr);
			}
		});
	});

	$('.downvote').click(function() {
		console.log('Ajax called');
		console.log($(this).data('postid'));
		const postID = $(this).data('postid');
		$.ajax({
			url : '/downvote',
			type : 'POST',
			contentType: 'application/json;charset=UTF-8',
			dataType: "json",
			data : JSON.stringify({'postid' : $(this).data('postid')}),
			success : function(response) {
				console.log(response);
				if (response.status === 'success'){
					console.log("Got that update!!!!!!!!!!");
					document.getElementById(postID).innerHTML ='&nbsp;<strong>' + response.upvotes.toString() + '</strong>&nbsp;';
				}

			},
			error : function(xhr) {
				console.log(xhr);
			}
		});
	});
});
