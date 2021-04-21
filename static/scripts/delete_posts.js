function delete_post(post_id) {
    var ask = window.confirm("Are you sure you want to delete this post?");
    if (ask) {
      window.alert("The post was successfully removed.")
      window.location.href = "/post/" + post_id + "/delete";
    }
  }