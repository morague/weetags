function close_notification(event) {
    let notification = event.target.parentNode;
    let container = event.target.parentNode.parentNode;
    container.removeChild(notification);
}

function open_notification(level, message) {
    let color = "is-success";
    if (level == "info") {
        color = "is-success";
    } else if (level == "warning") {
        color = "is-warning";
    } else if (level == "error") {
        color = "is-danger";
    }

    console.log(color)
    let container = document.getElementById("notifications");
    let notification = document.createElement("div");
    notification.classList.add("alert", "notification", "is-light", color);
    

    let btn = document.createElement("button");
    btn.classList.add("delete");
    btn.setAttribute("type", "button");
    btn.setAttribute("onclick", "close_notification(event)");

    let text = document.createElement("p");
    text.innerText = message;
    
    notification.appendChild(btn);
    notification.appendChild(text);
    container.appendChild(notification);
}