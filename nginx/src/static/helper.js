const form = document.getElementById('todo_form');
const todo_list = document.querySelector('.todo_list');
let ws = new WebSocket(`ws://${window.location.host}/ws`);
let reconnectInterval = 1000;
const maxReconnectInterval = 30000;

ws.onopen = () => {
    console.log("Successfully connected to Websocket Server I guess")
};

ws.onclose = (event) => {
    console.warn(`WebSocket closed. Code: ${event.code}. Reason: ${event.reason}`);
    if (event.code !== 1000) {
        console.log(`attempting reconnection`)

        setTimeout(() => {
            reconnectInterval = Math.min(reconnectInterval * 2, maxReconnectInterval);
            ws = new WebSocket(`ws://${window.location.host}/ws`);
        }, reconnectInterval);
    }
}

window.onbeforeunload = function() {
    ws.close(1000, "Page reloading");
};

ws.onmessage = (event) => { // websocket message recieved from client, updates page
    console.log(event)

    let payload = JSON.parse(event.data)
    console.log(payload)
    switch(payload[1]){
        case 'create':
            let todo_item = payload[0];
            createTodo(todo['id'], todo['todo']);
            break;
        case 'load':
            let data = payload[0];
            console.log(data);
            data.forEach(element => {
                createTodo(element[0], element[1], element[2]);
            });
            break;
        case 'update':
            let update = payload[0]
            updateTodo(update[0], update[1])
            break;
        case 'delete':
            let id = payload[0];
            document.getElementById(id.toString()).remove();
            break;
        default:
            console.log('Unknown Action was Recieved')
    }
};

form.addEventListener('submit', async function(event) {
    event.preventDefault();
    const formData = new FormData(this);
    // for (let [key, value] of formData.entries()) {
    //     console.log(`${key}: ${value}`);
    // }

    let [data] = formData.entries();
    //console.log(data);
    if (data[1] != '') {
        await fetch('api/submit', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            console.log(data);
            // createTodo(data);
        })
        .catch(error => {
            console.error("Error:", error)
        });

        form.reset();
    }
});

window.addEventListener("load", () => {
    console.log("page loaded! attempting to get database stuff...")
    fetch('api/load', {
        method: 'GET'
    })
    .then(response => response.json())
    .then(data => console.log(data))
    .catch(error => console.error("Error:", error));
})

todo_list.addEventListener('change', function(event){
    if (event.target && event.target.type === 'checkbox') {
        const checkbox = event.target;
        fetch('api/update', {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify((checkbox.checked) ? 
            {"id":checkbox.parentElement.id, "todo":"", "resolved":1} : 
            {"id":checkbox.parentElement.id, "todo":"", "resolved":0}),
        })
        .then(response => response.json())
        .then(data => console.log(data))
        .catch(error => console.error("Error:", error));
    }
});

function createTodo(id, todo, resolved = 0) {
    const todoDiv = document.createElement('div');
    console.log(id, todo, resolved);

    // note: 0=primarykey,1=todostring,2=resolved
    todoDiv.className = "todo_item";
    todoDiv.id = id.toString();
    todoDiv.innerHTML = `
    <input id="todo" name="resolve" type="checkbox">
    <label for="todo">${todo}</label>
    <button id="delete" onclick="deleteSelf(${id})" type="button">Delete</button>
    `;
    todo_list.appendChild(todoDiv);
    if (resolved == 1){
        todoDiv.querySelector('input').checked = true;
    }
}

function deleteSelf(id){
    console.log("self removal.", id)
    fetch('api/delete', {
        method: 'DELETE',
        body: id,
    })
    .then(response => response.json())
    .then(data => console.log(data))
    .catch(error => console.error("Error:", error));
}

function updateTodo(id, resolved){
    console.log("updating!", id)
    let parent_todo = document.getElementById(id.toString)
    let checkbox = document.querySelector('input[name="resolve"]')
    if (resolved == 1) {checkbox.checked = true;} else {checkbox.checked = false;}
}