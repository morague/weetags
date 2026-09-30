document.querySelectorAll(".fname").forEach(element => {
    element.addEventListener("click", event => {
            let e = event.target;
            while (e.classList.contains("fname") == false) {
                e = e.parentElement;
            }

            let checkbox = e.getElementsByTagName("input")[0];
            if (e.getAttribute("name") == "name") {
                checkbox.checked = true;
                e.classList.add("is-primary");
                return
            }
            
            if (checkbox.checked) {
                checkbox.checked = false;
                e.classList.remove("is-primary");
            } else {
                checkbox.checked = true;
                e.classList.add("is-primary");
            }


    })
});

document.getElementsByTagName("body")[0].addEventListener("click", event => {
    let popup = document.getElementById("popup");
    if (
        event.target == popup || 
        event.target.closest('.popup') ||
        event.target.classList.contains("kv") ||
        event.target.closest(".kv")
    ) {
        return
    }
    
    let display = popup.style.display;
    if (display == "block") {
        popup.style.display = "none";
        popup.getElementsByClassName("key")[0].innerText = "";
        popup.getElementsByClassName("content")[0].innerText = "";
    }
    
})

async function open_dropdown(e) {
    
    let dropdown = e.closest(".dropdown");
    if (dropdown.classList.contains("is-active")) {
        dropdown.classList.remove("is-active");
    } else {
        dropdown.classList.add("is-active");
    }
    
}

async function rfilter() {
    let field = document.createElement("div");
    field.classList.add("field");

    let control = document.createElement("div");
    control.classList.add("control");


    let tag = document.createElement("div");
    tag.classList.add("tags", "has-addons", "are-medium", "relation");

    let r = await selectable("relation", relations, "children", "filter-rel");
    let v = await  filter_input("value", placeholder="Node-name");
    let del = await filter_delete();
    tag.appendChild(r);
    tag.appendChild(v);
    tag.appendChild(del);

    control.appendChild(tag)
    field.appendChild(control)
    return field
}

async function ffilter() {
    let field = document.createElement("div");
    field.classList.add("field");

    let control = document.createElement("div");
    control.classList.add("control");


    let tag = document.createElement("div");
    tag.classList.add("tags", "has-addons", "are-medium", "filter");


    // select callable
    // input args that generate new input when filled
    let func = await selectable("func", sql_functions, "json_each", "filter-func");
    let path = await  filter_input("func_args", "Inner Path");
    let f = await selectable("func_args", fields, "name", "filter-fname");
    let op = await selectable("operator", operators, "=", "filter-op");
    let v = await  filter_input("value");
    let del = await filter_delete();

    tag.append(func);
    tag.appendChild(f);
    tag.appendChild(path);
    tag.appendChild(op);
    tag.appendChild(v);
    tag.appendChild(del);

    control.appendChild(tag)
    field.appendChild(control)
    return field

}

async function filter() {
    let field = document.createElement("div");
    field.classList.add("field");

    let control = document.createElement("div");
    control.classList.add("control");


    let tag = document.createElement("div");
    tag.classList.add("tags", "has-addons", "are-medium", "filter");
    
    let f = await selectable("fname", fields, "name", "filter-fname");
    let op = await selectable("operator", operators, "=", "filter-op");
    let v = await  filter_input("value");
    let del = await filter_delete();

    tag.appendChild(f);
    tag.appendChild(op);
    tag.appendChild(v);
    tag.appendChild(del);

    control.appendChild(tag)
    field.appendChild(control)
    return field
}

async function selectable(key, values, selected, cls) {
    let span = document.createElement("span");
    span.classList.add("tag", "is-rounded", "fvalue", "is-hoverable");
    span.setAttribute("key", key)

    let select = document.createElement("select");
    select.classList.add(cls, "arg")
    select.setAttribute("key", key)
    for (var i = 0; i < values.length; i++) {
        let v = values[i];
        let opt = document.createElement("option");
        if (selected == v) {
            opt.setAttribute("selected", true);
        }
        opt.innerText = v;
        opt.value = v;
        select.appendChild(opt);
    }
    span.appendChild(select);
    return span
}

async function filter_input(key, placeholder="Value", defautl_value=null) {
    let span = document.createElement("span");
    span.classList.add("tag", "is-rounded", "is-hoverable");
    

    let input = document.createElement("input");
    input.classList.add("filter-value", "arg");
    input.setAttribute("key", key)
    input.setAttribute("type", "text");
    input.placeholder = placeholder;
    input.value = defautl_value;
    span.appendChild(input);
    return span
}

async function filter_delete() {
    let span = document.createElement("span");
    span.classList.add("tag", "is-rounded", "is-delete", "is-hoverable");

    span.onclick = (e) => {
        let element = e.target.closest(".field");
        element.remove();
    };

    return span
}

async function add_r_filter(e) {
    let container = document.getElementById("filters");
    let f = await rfilter();
    await open_dropdown(e);
    container.append(f);

}


async function add_f_filter(e) {
    let container = document.getElementById("filters");
    let f = await ffilter();
    await open_dropdown(e);
    container.append(f);

}

async function add_filter(e) {
    let container = document.getElementById("filters");
    let f = await filter();
    await open_dropdown(e);
    container.append(f);
}

async function filters(cls) {
    var conditions = [];
    let filters = document.getElementById("filters").getElementsByClassName(cls);
    for (var i = 0; i < filters.length; i++) {
        let f = filters[i]
        let arg_containers = f.getElementsByClassName("arg");
        let block = {};
        for (var j = 0; j < arg_containers.length; j++) {
            let c = arg_containers[j];
            if (c.tagName == "INPUT") {
                var arg = c.value || null;
                var key = c.getAttribute("key");
            } else if (c.tagName == "SELECT") {
                var arg = await get_selected_value(c);
                var key = c.getAttribute("key");
            }

            if (key == "func_args" && block[key] === undefined) {
                block[key] = [arg]
            } else if (key == "func_args") {
                block[key].push(arg)
            } else {
                block[key] = arg
            }
        }
        conditions.push(block);
    }   
    return conditions
}

async function fselected() {
    let fields = document.getElementById("fields").getElementsByClassName("fname");
    let selected = [];
    for (var i = 0; i < fields.length; i++) {
        let f = fields[i];

        let checked = f.getElementsByTagName("input")[0].checked;
        if (checked) {
            let fname = f.getAttribute("name");
            selected.push(fname);
        }
    }
    return selected
}

async function request(endpoint, method = "POST", body=null, headers=null) {
    args = {method: method}
    if (body != null) {
        args.body = body
    }
    if (headers != null) {
        args.headers = headers
    }

    try {
        const response = await fetch(endpoint, args);
        return response
    } catch (e) {
        console.log(e);
        open_notification("error", "An unexpected error happened.");
        return null
    }
}

async function search(tree_name, page=0) {
    await create_loader();

    let endpoint = "/v1/trees/"+tree_name+"/nodes";
    let q = await filters("filter");
    let r = await filters("relation");
    let fields = await fselected();
    let page_size = document.getElementById("results").getAttribute("page_size");
    let body = JSON.stringify({"q": q, "r": r, "fields": fields, "page": page, "page_size": parseInt(page_size)});
    console.log(body);
    let headers = new Headers({ "Accept":"application/json", "Content-Type":"application/json" });
    let response = await request(endpoint, "POST", body, headers);


    if (response == null) {
        await create_placeholder();
    } else if (response.status == 200) {
        let r = await response.json();
        await display_results(r.data);
        document.getElementById("results").setAttribute("page", page);
        document.getElementById("current-page").innerText = page;
    } else {
        let r = await response.json();
        open_notification("error", r.reasons);
        await create_placeholder();
        return 500
    }
}

async function previous_page(tree_name) {
    let results = document.getElementById("results");
    let current_page = parseInt(results.getAttribute("page"));

    if (current_page == 0) {
        open_notification("error", "Already reading page 0.");
        return
    }
    await search(tree_name, current_page - 1);
}

async function next_page(tree_name) {
    let results = document.getElementById("results");
    let current_page = parseInt(results.getAttribute("page"));
    await search(tree_name, current_page + 1);
}

async function get_selected_value(element) {
    let selected = null;
    let options = element.getElementsByTagName("option");
    for (var i = 0; i < options.length; i++) {
        let opt = options[i];
        if (opt.selected) {
            selected = opt.value;
        }
    }
    return selected
}


async function popup_content(element) {
    let popup = document.getElementById("popup");

    let display = popup.style.display;
    if (display == "none" || display == "") {
        popup.getElementsByClassName("key")[0].innerText = element.getAttribute("key");
        popup.getElementsByClassName("content")[0].innerText = element.getAttribute("value");
        element.classList.add("poped");
        popup.style.display =  "block";
    } else {
        popup.style.display = "none";
        popup.getElementsByClassName("key")[0].innerText = "";
        popup.getElementsByClassName("content")[0].innerText = "";
        element.classList.remove("poped");
    }
}

async function display_results(data) {
    let container = document.getElementById("results");
    container.innerHTML = "";
    if (data.length > 0) {
        for (var i = 0; i < data.length; i++) {
            let d = data[i];
            let e =  await node(d);
            container.appendChild(e);
        }
    } else {
        await create_placeholder();
    }
}

const isObject = (value) => {
  return typeof value === 'object'
  && value !== null
  && !Array.isArray(value)
  && !(value instanceof RegExp)
  && !(value instanceof Date)
  && !(value instanceof Set)
  && !(value instanceof Map)
}


async function node(data) {
    let container = document.createElement("div");
    container.classList.add("box", "result");
    container.setAttribute("value", data.name);

    let columns = document.createElement("div");
    columns.classList.add("columns", "is-vcentered");

    columns.innerHTML += '<div class="column is-4"><strong>'+data.name+'</strong></div>'

    let d = document.createElement("div");
    d.classList.add("column", "is-8");

    let fbox = document.createElement("div");  
    fbox.classList.add("is-flex", "is-flex-wrap-wrap", "is-justify-content-space-between");

    for (let [key, value] of Object.entries(data)) {
        let tag = document.createElement("div");
        tag.classList.add("kv", "is-flex");
        tag.setAttribute("key", key);
        if (Array.isArray(value)) {
            tag.setAttribute("value", value.join(", "));
        } else if (isObject(value)) {
            value = JSON.stringify(value);
            tag.setAttribute("value", value);
        } else {
            tag.setAttribute("value", value);
        }
        tag.onclick = (e) => {
            let element = e.target.closest('.kv');
            popup_content(element);            
        };

        tag.innerHTML += '<div class="k"><strong>'+key+'</strong></div>'
        tag.innerHTML += '<div class="v">'+value+'</div>'
        fbox.appendChild(tag);
    }

    d.appendChild(fbox);
    columns.appendChild(d);
    container.appendChild(columns);
    return container
}

async function create_placeholder() {
    let results = document.getElementsByClassName("result");
    if (results.length == 0) {
        let container = document.getElementById("results");
        container.innerHTML = `
        <div class="box placeholder">
            <div class="columns">
                <div class="column has-text-centered"><strong>No results</strong></div>
            </div>
        </div>
        `
    }
}

async function create_loader() {
    let container = document.getElementById("results");
    container.innerHTML = `
    <div id="weetags-loader" class="box loader-box">
        <div class="loader"></div>
    </div>
    `
}

async function change_page_size(element) {
    let opts = element.getElementsByTagName("option");
    for (let i = 0; i < opts.length; i++) {
        let opt = opts[i];
        if (opt.selected == true) {
            document.getElementById("results").setAttribute("page_size", opt.value);
        }
    }
}
