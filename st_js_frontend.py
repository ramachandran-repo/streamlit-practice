<html lang="en">
<head>
<style>
  .attn-boxes-container {
    display: flex;
    align-items: flex-start;
    flex-wrap: nowrap;
  }

  .core-container {
    display: flex;
    align-items: flex-start;
    flex-wrap: nowrap;
  }

  .core-container {
  overflow: auto;
  white-space: nowrap;
}

/*div.scrollmenu a {
  display: inline-block;
  color: white;
  text-align: center;
  padding: 14px;
  text-decoration: none;
}

div.scrollmenu a:hover {
  background-color: #777;
}
*/
  .attn-button-pos {
    margin: auto;
    width: 50%;
    padding: 10px;
  }
  
  .rtnl-button-pos {
    margin: auto;
    width: 50%;
    padding: 10px;
  }

  .emtl-button-pos {
    margin: auto;
    width: 50%;
    padding: 10px;
  }

  .nxt-button-pos {
    margin: auto;
    width: 50%;
    padding: 10px;
  }
  

  .rtnl-boxes-container {
    display: flex;
    align-items: flex-start;
    flex-wrap: nowrap;
  }
  
  .emtl-boxes-container {
    display: flex;
    align-items: flex-start;
    flex-wrap: nowrap;
  }
  
  .nxt-boxes-container {
    display: flex;
    align-items: flex-start;
    flex-wrap: nowrap;
  }

  .box {
    border: 2px solid red;
    border-radius: 8px;
    padding: 8px;
    width: 250px;
    margin-right: 10px;
    position: relative;
    cursor: pointer;
    display: inline-block;
  }

  .title-bar {
    display: flex;
    justify-content: flex-start;
    margin-bottom: 8px;
  }

  .title-bar div,
  .close {
    width: 15px;
    height: 15px;
    border-radius: 50%;
    display: inline-block;
    margin-right: 5px;
  }

  .close {
    background-color: red;
    color: white;
    text-align: center;
    line-height: 15px;
    cursor: pointer;
    position: absolute;
    right: 5px;
    top: 5px;
  }

  .add-attn-box {
    padding: 10px 20px;
    cursor: pointer;
    font-size: 24px;
    line-height: 15px;
    display: flex;
    align-items: center;
    user-select: none;
  }
  
    .add-rtnl-box {
    padding: 10px 20px;
    cursor: pointer;
    font-size: 24px;
    line-height: 15px;
    display: flex;
    align-items: center;
    user-select: none;
  }
  
   .add-emtl-box {
    padding: 10px 20px;
    cursor: pointer;
    font-size: 24px;
    line-height: 15px;
    display: flex;
    align-items: center;
    user-select: none;
  }
  
   .add-nxt-box {
    padding: 10px 20px;
    cursor: pointer;
    font-size: 24px;
    line-height: 15px;
    display: flex;
    align-items: center;
    user-select: none;
  }

  .popup-overlay {
    display: none;
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0, 0, 0, 0.5);
    z-index: 1000;
  }

  .popup {
    display: none;
    position: fixed;
    top: 25%;
    left: 50%;
    transform: translate(-50%, -50%);
    background-color: white;
    padding: 50px;
    z-index: 1000;
    border: 1px solid #ccc;

  }
  
  .popup form {
    display: flex;
    flex-direction: column;
  }

  .popup label {
    margin-bottom: 5px;
  }

  .popup textarea {
    width: 100%;
    box-sizing: border-box;
  }







  .content_container {
    display: flex;
    align-items: center;
  }

  .content_option-box {
    border: 1px solid black;
    padding: 10px;
    margin-right: 10px;
  }

  .content_add-button {
    cursor: pointer;
    border: 1px solid black;
    padding: 5px;
    border-radius: 50%;
    text-align: center;
    line-height: 20px;
    width: 20px;
    height: 20px;
    user-select: none;
  }

  .content_options-menu {
    display: none;
    position: absolute;
    margin-top: 25px;
    border: 1px solid #000;
    background: #fff;
  }

  .content_options-menu button {
    display: block;
    padding: 5px 10px;
    border: none;
    background: none;
    cursor: pointer;
    text-align: left;
    width: 100%;
  }

  .content_options-menu button:hover {
    background-color: #f0f0f0;
  }









 .channel_container {
    width: 100%;
  }

  .row {
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
  }

  .box, .textarea-box {
    border: 1px solid black;
    padding: 10px;
    flex: 1;
    margin-right: 10px; /* Adjust space between the option box and textarea */
  }

  .textarea-box {
    margin-right: 0; /* No margin for the textarea on the right side */
  }

  .channel_add-button {
    padding: 10px;
    border: 1px solid black;
    background-color: #e7e7e7;
    cursor: pointer;
    text-align: center;
    width: 49%; /* Adjust this to match the first element of the row above */
  }

  .channel_options-menu {
    display: none;
    position: absolute;
    list-style-type: none;
    padding: 0;
    margin-top: 5px; /* Adjust this to position the menu */
    border: 1px solid black;
    background-color: white;
    width: 49%; /* Match width to the add-button */
  }

  .channel_options-menu li {
    padding: 5px 10px;
    cursor: pointer;
  }

  .channel_options-menu li:hover {
    background-color: #f0f0f0;
  }





</style>
</head>
<body>
<h3>Get Attention</h3>
<div class="core-container" id="core-container">
<div class="attn-boxes-container" id="attn-boxes-container">
  <!-- Boxes will be added here -->

</div>

<!-- <div class="add-attn-box" onclick="addAttnBox()">+</div> -->
<div class="attn-button-pos">
<input type="button" value="+" onclick="addAttnBox()">
</div>
</div>
<div class="popup-overlay" id="popup-overlay" onclick="closePopup()"></div>

<div class="popup" id="popup">
  <div class="core_message">
    <label for="core-message">Core Message:</label>
    <textarea id="core_message" name="core_message" rows="2"></textarea>
  </div>
  <div class="topics">
    <label for="topiclabel">Topics:</label>
    <div class="content_container">
      <div class="content_add-button" onclick="toggleOptionsMenu()">+</div>
    </div>

    <div class="content_options-menu" id="content_options-menu">
      <button onclick="addOptionBox('Option 1')">Option 1</button>
      <button onclick="addOptionBox('Option 2')">Option 2</button>
      <button onclick="addOptionBox('Option 3')">Option 3</button>
      <button onclick="addOptionBox('Option 4')">Option 4</button>
      <button onclick="addOptionBox('Option 5')">Option 5</button>
    </div>
  </div>
  <div class="channel">
    <div class="channel_container">
      <div class="row">
        <!-- First two boxes will be here -->
      </div>
      <div class="row">
        <div class="channel_add-button" onclick="addNewChannel()">add new channel</div>
      </div>
    </div>

    <ul class="channel_options-menu" id="channel_options-menu">
      <li onclick="selectOption(this, 'Option 1')">Option 1</li>
      <li onclick="selectOption(this, 'Option 2')">Option 2</li>
      <li onclick="selectOption(this, 'Option 3')">Option 3</li>
    </ul>
  </div>
  <div class="popup_buttons">
    <button type="button" onclick="submitForm()">Submit</button>
    <button type="button" onclick="closePopup()">Cancel</button>
  </div>
</div>

<h3>Get rational</h3>
<div class="core-container" id="core-container">
<div class="rtnl-boxes-container" id="rtnl-boxes-container">
  <!-- Boxes will be added here -->

</div>

<div class="rtnl-button-pos">
<input type="button" value="+" onclick="addRtnlBox()">
</div>
</div>


<div class="popup-overlay" id="popup-overlay" onclick="closePopup()"></div>


<h3>Get Emotional</h3>
<div class="core-container" id="core-container">
<div class="emtl-boxes-container" id="emtl-boxes-container">
  <!-- Boxes will be added here -->

</div>

<div class="emtl-button-pos">
<input type="button" value="+" onclick="addEmtlBox()">
</div>
</div>

<div class="popup-overlay" id="popup-overlay" onclick="closePopup()"></div>



<h3>Next Steps</h3>
<div class="core-container" id="core-container">
<div class="nxt-boxes-container" id="nxt-boxes-container">
  <!-- Boxes will be added here -->

</div>
<div class="nxt-button-pos">
<input type="button" value="+" onclick="addNxtBox()">
</div>
</div>

<div class="popup-overlay" id="popup-overlay" onclick="closePopup()"></div>

<script>

let attnBoxId = 0;
let rtnlBoxId = 0;
let emtlBoxId = 0;
let nxtBoxId = 0;

function createAttnBox() {
  attnBoxId++;
  const box = document.createElement('div');
  box.className = 'box';
  box.innerHTML = `
    <div class="title-bar" onclick="openPopup()">
      <div style="background-color: green;"></div>
      <div style="background-color: yellow;"></div>
      <div class="close" onclick="closeBox(event, ${attnBoxId})">X</div>
    </div>
    <div class="content">
      Core message Attn #${attnBoxId}
    </div>
  `;
  box.onclick = function(event) {
    // Prevent the close button's click from triggering this
    if (event.target === box || event.target === box.querySelector('.content')) {
      openPopup();
    }
  };
  return box;
}

function addAttnBox() {
  const attnBoxesContainer = document.getElementById('attn-boxes-container');
  if(attnBoxesContainer){
  const last = Array.from(document.getElementsByClassName('attn-boxes-container')).pop();
  }
  attnBoxesContainer.appendChild(createAttnBox());
}

// Initialize with 2 boxes

addAttnBox();
addAttnBox();


function createRtnlBox() {
  rtnlBoxId++;
  const box = document.createElement('div');
  box.className = 'box';
  box.innerHTML = `
    <div class="title-bar">
      <div style="background-color: green;"></div>
      <div style="background-color: yellow;"></div>
      <div class="close" onclick="closeBox(event, ${rtnlBoxId})">X</div>
    </div>
    <div class="content">
      Core message Rtnl #${rtnlBoxId}
    </div>
  `;
  box.onclick = function(event) {
    // Prevent the close button's click from triggering this
    if (event.target === box || event.target === box.querySelector('.content')) {
      openPopup();
    }
  };
  return box;
}

function addRtnlBox() {
  const rtnlBoxesContainer = document.getElementById('rtnl-boxes-container');
  if(rtnlBoxesContainer){
  const last = Array.from(document.getElementsByClassName('rtnl-boxes-container')).pop();
  }
  rtnlBoxesContainer.appendChild(createRtnlBox());
}

// Initialize with 2 boxes
addRtnlBox();
addRtnlBox();


function createEmtlBox() {
  emtlBoxId++;
  const box = document.createElement('div');
  box.className = 'box';
  box.innerHTML = `
    <div class="title-bar">
      <div style="background-color: green;"></div>
      <div style="background-color: yellow;"></div>
      <div class="close" onclick="closeBox(event, ${emtlBoxId})">X</div>
    </div>
    <div class="content">
      Core message Emtl #${emtlBoxId}
    </div>
  `;
  box.onclick = function(event) {
    // Prevent the close button's click from triggering this
    if (event.target === box || event.target === box.querySelector('.content')) {
      openPopup();
    }
  };
  return box;
}

function addEmtlBox() {
  const emtlBoxesContainer = document.getElementById('emtl-boxes-container');
  if(emtlBoxesContainer){
  const last = Array.from(document.getElementsByClassName('emtl-boxes-container')).pop();
  }
  emtlBoxesContainer.appendChild(createEmtlBox());
}

// Initialize with 2 boxes
addEmtlBox();
addEmtlBox();

function createNxtBox() {
  nxtBoxId++;
  const box = document.createElement('div');
  box.className = 'box';
  box.innerHTML = `
    <div class="title-bar">
      <div style="background-color: green;"></div>
      <div style="background-color: yellow;"></div>
      <div class="close" onclick="closeBox(event, ${nxtBoxId})">X</div>
    </div>
    <div class="content">
      Core message Emtl #${nxtBoxId}
    </div>
  `;
  box.onclick = function(event) {
    // Prevent the close button's click from triggering this
    if (event.target === box || event.target === box.querySelector('.content')) {
      openPopup();
    }
  };
  return box;
}

function addNxtBox() {
  const nxtBoxesContainer = document.getElementById('nxt-boxes-container');
  if(nxtBoxesContainer){
  const last = Array.from(document.getElementsByClassName('nxt-boxes-container')).pop();
  }
  nxtBoxesContainer.appendChild(createNxtBox());
}

// Initialize with 2 boxes
addNxtBox();
addNxtBox();


function closeBox(event, id) {
  event.stopPropagation();
  const box = event.target.closest('.box');
  if (box) {
    box.remove();
  }
}


function sendMessageToStreamlitClient(type, data) {
      var outData = Object.assign({
        isStreamlitMessage: true,
        type: type,
      }, data);
      window.parent.postMessage(outData, "*");
    }

function init() {
  sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1});
}


// The `data` argument can be any JSON-serializable value.
function sendDataToPython(data) {
  sendMessageToStreamlitClient("streamlit:setComponentValue", data);
}

// data is any JSON-serializable value you sent from Python,
// and it's already deserialized for you.
function onDataFromPython(event) {
  if (event.data.type !== "streamlit:render") return;
  core_message.value = event.data.args.my_input_value["core_message"];  // Access values sent from Python here!
}

//   // Hook things up!
window.addEventListener("message", onDataFromPython);
init();

// Hack to autoset the iframe height.
window.addEventListener("load", function() {
  window.setTimeout(function() {
    // const element = document.getElementById("core-container");
    // let clientHeight = element.clientHeight;
    // let clientWidth = element.clientWidth;
    // setFrameHeight(document.documentElement.clientHeight)
    // setFrameHeight(document.documentElement.clientHeight)
    setFrameHeight(750)
    // sendMessageToStreamlitClient("streamlit:setFrameWidth", {width: 700});
    document.documentElement.width = "400";
    document.getElementById("core-container").width = 400
  }, 0);
});

function sendMessageToStreamlitClient(type, data) {
        var outData = Object.assign({
          isStreamlitMessage: true,
          type: type,
        }, data);
        window.parent.postMessage(outData, "*");
      }

function init() {
  sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1});
}

function setFrameHeight(height) {
  sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: height});
}

function openPopup() {
  document.getElementById('popup').style.display = 'block';
  document.getElementById('overlay').style.display = 'block';
}

function closePopup() {
  document.getElementById('popup').style.display = 'none';
  document.getElementById('overlay').style.display = 'none';
}

function sendDataToPython(data) {
        sendMessageToStreamlitClient("streamlit:setComponentValue", data);
      }


function submitForm() {
  var core_message = document.getElementById('core_message').value;
  var place = document.getElementById('place').value;
  sendDataToPython({
          value: {"core_message": core_message, "place": place},
          dataType: "json",
        });
  closePopup();
}

function closeWindow() {
  document.getElementById('window').style.display = 'none';
}


// ===============================================

function toggleOptionsMenu() {
  var menu = document.getElementById("content_options-menu");
  menu.style.display = menu.style.display === "block" ? "none" : "block";
}

function addOptionBox(value) {
  var container = document.querySelector(".content_container");
  var newBox = document.createElement("div");
  newBox.classList.add("content_option-box");
  newBox.textContent = value;
  container.insertBefore(newBox, container.lastElementChild);
  toggleOptionsMenu();
}

// Close the options menu if clicked outside of it
document.addEventListener('click', function(event) {
  var isClickInsideMenu = document.getElementById("content_options-menu").contains(event.target);
  var isAddButton = document.querySelector(".content_add-button").contains(event.target);
  if (!isClickInsideMenu && !isAddButton) {
    document.getElementById("content_options-menu").style.display = 'none';
  }
});


// =================================================


function addNewChannel() {
  // Create a new row
  const newRow = document.createElement('div');
  newRow.className = 'row';

  // Create the first box with options
  const optionBox = document.createElement('div');
  optionBox.className = 'box';
  optionBox.onclick = function(event) {
    // Position the options menu relative to the option box
    const optionsMenu = document.getElementById('channel_options-menu');
    optionsMenu.style.display = 'block';
    optionsMenu.style.left = `${optionBox.getBoundingClientRect().left}px`;
    optionsMenu.style.top = `${optionBox.getBoundingClientRect().bottom + window.scrollY}px`;
    // Set the current option box as the target for the options menu
    optionsMenu.setAttribute('data-target', optionBox.innerText);
  };
  newRow.appendChild(optionBox);

  // Create the second box as a textarea
  const textareaBox = document.createElement('textarea');
  textareaBox.className = 'textarea-box';
  newRow.appendChild(textareaBox);

  // Add the new row before the row with the add button
  const container = document.querySelector('.channel_container');
  container.insertBefore(newRow, container.children[container.children.length - 1]);
}

function selectOption(item, option) {
  const optionsMenu = document.getElementById('channel_options-menu');
  const targetText = optionsMenu.getAttribute('data-target');
  const boxes = document.querySelectorAll('.box');

  // Find the box that was clicked and set its text content to the selected option
  boxes.forEach(box => {
    if (box.innerText === targetText) {
      box.innerText = option;
    }
  });

  // Hide the options menu
  optionsMenu.style.display = 'none';
}

// Close the options menu if clicked outside of it
document.addEventListener('click', function(event) {
  const optionsMenu = document.getElementById("channel_options-menu");
  const isClickInsideMenu = optionsMenu.contains(event.target);
  const isOptionBox = event.target.classList.contains('box');

  if (!isClickInsideMenu && !isOptionBox) {
    optionsMenu.style.display = 'none';
  }
});

</script>

</body>
</html>
