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

ws.onmessage = (event) => { // websocket message recieved from client, updates page
    console.log(event)
    let todo = JSON.parse(JSON.parse(event.data))
    createTodo(todo[0], todo[1])
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
        // await fetch('/submit', {
        //     method: 'POST',
        //     body: formData
        // })
        // .then(response => response.json())
        // .then(data => {
        //     console.log(data);
        //     // createTodo(data);
        // })
        // .catch(error => {
        //     console.error("Error:", error)
        // });

        console.log("about to send form through websocket", formData);
        ws.send(JSON.stringify(Object.fromEntries(formData.entries())));
        console.log("sent i think");

        form.reset();
    }
});

window.addEventListener("load", () => {
    console.log("page loaded! attempting to get database stuff...")
    fetch('/load', {
        method: 'GET'
    })
    .then(response => response.json())
    .then(data => {
        console.log(data);
        data.forEach(element => {
            createTodo(element[0], element[1], element[2]);
        });
    })
})

todo_list.addEventListener('change', function(event){
    if (event.target && event.target.type === 'checkbox') {
        const checkbox = event.target;
        fetch('/update', {
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
    fetch('/delete', {
        method: 'DELETE',
        body: id,
    })
    .then(response => response.json())
    .then(data => {
        console.log(data);
        if (data == "deleted") {
            document.getElementById(id.toString()).remove();
        }
    })
}