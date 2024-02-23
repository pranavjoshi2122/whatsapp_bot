const { BufferJSON, WA_DEFAULT_EPHEMERAL, generateWAMessageFromContent, proto, generateWAMessageContent, generateWAMessage, prepareWAMessageMedia, areJidsSameUser, getContentType } = require("@adiwajshing/baileys");
const fs = require("fs");
const util = require("util");
const chalk = require("chalk");
const { Configuration, OpenAIApi } = require("openai");
const sqlite3 = require('sqlite3').verbose();
const prefixes = ['Cicero', 'cicero', 'Help', 'help'];
const authorizedNumbers = ['917878129383',];  // Numbers of Aurel Robert


let setting = require("./key.json");
const { unsupportedMediaType } = require("@hapi/boom");
const { property } = require("lodash");

function getCurrentDateAndTimeInGMT6() {
  const date = new Date();
  const options = {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false, timeZone: 'Etc/GMT+6'
  };
  return new Intl.DateTimeFormat('de-DE', options).format(date);
}

function getCurrentDate() {
  const date = new Date();
  const options = {
    year: 'numeric', month: '2-digit', day: '2-digit',
    timeZone: 'Etc/GMT+6'
  };
  return new Intl.DateTimeFormat('de-DE', options).format(date).split(',')[0];
}

function getWeekNumber(d) {
  d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return weekNo;
}

// Check if the logging has already been initialized
if (!global.loggingInitialized) {
  global.loggingInitialized = true;

  // Global variables for logging
  let lastLoggedDate = '';
  let logQueue = [];
  let lastLoggedMessage = '';
  let lastLogTime = 0;
  const logDebounceInterval = 500; // Adjust the debounce interval as needed
  const originalConsoleLog = console.log;
  let isWritingLog = false; // Initialize isWritingLog as false

  // Function to format the message with date and time
  function formatMessageWithDateTime(message) {
    return `${getCurrentDateAndTimeInGMT6()} - ${message}`;
  }

  // Function to process the log queue
  function processLogQueue() {
    if (logQueue.length === 0 || isWritingLog) {
      return;
    }

    isWritingLog = true;
    const message = logQueue.shift();

    const weekNumber = getWeekNumber(new Date());
    const logFileName = `./logs_week_${weekNumber}.txt`;
    const logStream = fs.createWriteStream(logFileName, { flags: 'a' });

    logStream.write(message + '\n', () => {
      logStream.end();
      isWritingLog = false;
      processLogQueue(); // Process the next message in the queue
    });
  }

  // Override console.log
  console.log = function (message) {
    // Debounce mechanism to avoid logging duplicates in quick succession
    const now = new Date().getTime();
    if (message === lastLoggedMessage && now - lastLogTime < logDebounceInterval) {
      return; // Skip logging if the same message is repeated too soon
    }
    lastLoggedMessage = message;
    lastLogTime = now;

    const hasDateTime = /\d{2}\.\d{2}\.\d{4}, \d{2}:\d{2}:\d{2}/.test(message);
    let finalMessage = hasDateTime ? message : formatMessageWithDateTime(message);

    // Filter out messages related to reconnection
    const reconnectionMessages = [
      "Connection Lost from Server, reconnecting...",
      "using WA v",
      "Bot successfully connected to server",
      "Type /menu to see menu"
    ];

    // Check if the message contains any of the reconnection messages
    if (reconnectionMessages.some(reconnMsg => message.includes(reconnMsg))) {
      return; // Skip adding these messages to the log
    }

    // Check for a new day to add separator
    const formattedDate = getCurrentDate();
    if (lastLoggedDate !== formattedDate) {
      if (lastLoggedDate !== '') {
        finalMessage = "\n\n\nNEXT DAY - NEXT DAY - NEXT DAY - NEXT DAY - NEXT DAY - NEXT DAY\n\n" + finalMessage;
      }
      lastLoggedDate = formattedDate;
    }

    // Add three line breaks before every line that includes '[ LOGS ]'
    if (message.includes('[ LOGS ]')) {
      finalMessage = "\n\n\n------------\n\n\n\n" + finalMessage; // Three line breaks added here
    }
    originalConsoleLog.call(console, finalMessage); // Original console log

    logQueue.push(finalMessage); // Add to log queue
    processLogQueue(); // Process the log queue
  };

  function logError(error) {
    let errorMessage = (typeof error === 'object' && error !== null) ? JSON.stringify(error, null, 2) : error;
    console.log("[ERROR] " + errorMessage);
  }
}

// Outside your sansekai function, initialize an object to track user results
let results = [];
let userResultsData = {};

// Define counters and flags
let watchFileCounter = 0;
let unwatchFileCounter = 0;
let isWatching = false; // flag to indicate whether the file is currently being watched

// Store references to original methods
const originalWatchFile = fs.watchFile;
const originalUnwatchFile = fs.unwatchFile;

// Replace fs.watchFile with a wrapper
fs.watchFile = function (...args) {
  if (!isWatching) { // Only watch if not already watching
    isWatching = true;
    watchFileCounter++;  // Increment counter
    console.log(`watchFile has been called ${watchFileCounter} times`);
    return originalWatchFile.apply(fs, args);  // Call original method
  }
};

// Replace fs.unwatchFile with a wrapper
fs.unwatchFile = function (...args) {
  if (isWatching) { // Only unwatch if currently watching
    isWatching = false;
    unwatchFileCounter++;  // Increment counter
    console.log(`unwatchFile has been called ${unwatchFileCounter} times`);
    return originalUnwatchFile.apply(fs, args);  // Call original method
  }
};

// Your existing code with the fs.watchFile usage
let file = require.resolve(__filename);
fs.watchFile(file, () => {
  fs.unwatchFile(file);
  console.log(chalk.redBright(`Update ${__filename}`));
  delete require.cache[file];
  require(file);
});


module.exports = sansekai = async (client, m, chatUpdate, store) => {
  try {
    // Get sender's phone number
    const senderNumber = m.sender.replace("@s.whatsapp.net", "");

    // Define body without assigning a value
    var body;

    if (m.mtype === "conversation") {
      body = m.message.conversation;
    } else if (m.mtype == "imageMessage") {
      body = m.message.imageMessage.caption;
    } else if (m.mtype == "videoMessage") {
      body = m.message.videoMessage.caption;
    } else if (m.mtype == "extendedTextMessage") {
      body = m.message.extendedTextMessage.text;
    } else if (m.mtype == "buttonsResponseMessage") {
      body = m.message.buttonsResponseMessage.selectedButtonId;
    } else if (m.mtype == "listResponseMessage") {
      body = m.message.listResponseMessage.singleSelectReply.selectedRowId;
    } else if (m.mtype == "templateButtonReplyMessage") {
      body = m.message.templateButtonReplyMessage.selectedId;
    } else if (m.mtype === "messageContextInfo") {
      body = m.message.buttonsResponseMessage?.selectedButtonId || m.message.listResponseMessage?.singleSelectReply.selectedRowId || m.text;
    }

    // Check if sender's phone number is authorized
    if (!authorizedNumbers.includes(senderNumber)) {
      /*m.reply(`*Thank you for you interest in Cicero (beta version) - Fast, simple, and clear property search in Costa Rica.* 

Access to Cicero is limited to our subscribers. 
We'll contact you very shortly! 

_Cicero is a product from Emperia Technologies S.R.L., based in Potrero, Guanacaste._`);*/
      console.log(`\n\nIgnored message from unauthorized number: ${senderNumber}`);
      return;
    }

    //  Process a message received via WhatsApp 
    var budy = typeof m.text == "string" ? m.text : "";
    // var prefix = /^[\\/!#.]/gi.test(body) ? body.match(/^[\\/!#.]/gi) : "/"
    // var prefix = /^[\\/!#.]/gi.test(body) ? body.match(/^[\\/!#.]/gi) : "/";
    // Split body into words
    var words = body.trim().split(/\s+/); // Splits by one or more spaces
    var firstWord = words[0].toLowerCase(); // Converts the first word to lower case for case-insensitive comparison
    const isCmd2 = body && prefixes.some(prefix => body.startsWith(prefix));
    if (isCmd2) {
      if (firstWord.endsWith(",") || firstWord.endsWith(":") || firstWord.endsWith(";") || firstWord.endsWith("-") || firstWord.endsWith(":")) {
        firstWord = firstWord.slice(0, -1);
      }
    }
    // Set prefix based on the first word
    if (["cicero", "help"].includes(firstWord)) {
      var prefix = firstWord; // Assign the original case of the first word
    } else {
      var prefix = ""; // Leave prefix empty
    }
    let command = prefix;
    const args = body.trim().split(/ +/);
    const pushname = m.pushName || "No Name";
    try {
      botNumber = await client.decodeJid(client.user.id);
    } catch (error) {
      console.error("Error retrieving bot number:", error);
      // Handle the error appropriately, maybe set botNumber to a default or handle the flow differently
    }
    const itsMe = m.sender == botNumber ? true : false;
    let text = args.join(" ");
    const arg = budy.trim().substring(budy.indexOf(" ") + 1);
    const arg1 = arg.trim().substring(arg.indexOf(" ") + 1);
    const from = m.chat;
    const reply = m.reply;
    const sender = m.sender;
    const mek = chatUpdate.messages[0];

    const color = (text, color) => {
      return !color ? chalk.green(text) : chalk.keyword(color)(text);
    };

    // Group
    let groupMetadata = "";
    if (m.isGroup) {
      try {
        groupMetadata = await client.groupMetadata(m.chat);
      } catch (error) {
        console.error("Error retrieving group metadata:", error);
        // Handle the error appropriately here
        // For example, you might choose to set groupMetadata to a default value or handle the flow differently
      }
    }

    const groupName = m.isGroup ? groupMetadata.subject : "";

    let argsLog = budy.length > 30 ? `${budy.substring(0, 30)}...` : budy;

    if (isCmd2 && !m.isGroup) {
      console.log("[ LOGS ]" + " From: " + pushname + ` [ ${m.sender.replace("@s.whatsapp.net", "")} ]`);
    } else if (isCmd2 && m.isGroup) {
      console.log("[ LOGS ]" + " From: ", pushname + ` [ ${m.sender.replace("@s.whatsapp.net", "")} ]` + " IN ", groupName);
    }

    // Check if the message starts with one of the prefixes
    if ((!body || !prefixes.some(prefix => body.startsWith(prefix))) && senderNumber !== "50661787160") {
      // If the message doesn't start with a prefix, 
      try {
        //if (!text) return reply(`Search for properties by simply asking for what you're looking for! Example:\n${prefix}${command} A house with 3 bedrooms in Potrero, ocean view, 600-900k`);

        const openai2 = new OpenAIApi(new Configuration({ apiKey: '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' })); // Second OpenAI instance

        // Send the user's request to the 2nd OpenAI instance
        const systemPrompt = "Firstly, if the user message does not appear to be a property search query or is clearly not intended for property searching, respond with 'No search query'. Otherwise, please extract the locations, type, price, number of bedrooms, number of bathrooms, or key features from the following text and provide a response formatted as follows: 'LocationA: (text containing one town, neighborhood, or gated community; add LocationB and so on for other locations), TypeA: (text containing one property type; add TypeB and so one for other types), Price: (integer number, with numerical expression classifier), Feature1: (text containing one key feature of the house, add Feature2 and so on for other features), Bedrooms: (float number, with numerical expression classifier), Bathrooms: (float number, with numerical expression classifier)'. Note that the criteria may appear in random order in the attached text. For the Location, there may be multiple locations in the request, in which case assign each of them to a separate field in your response. Still for the Location, note that in indications such as 'close to LocationA', or 'near LocationA', or 'LocationA and surroundings', only the location must appear in your response. Still for the Location, note that the following terms should be interpreted as locations: Hacienda Pinilla, Catalina Cove, Senderos, Altos de Flamingo, Reserva Conchal, Las Ventanas, Tamarindo Park. For the Type, it can be either a House or a Condo; note that the following words must always be replaced by the word 'House': 'Home', 'Villa', 'Townhouse', 'Ranch', 'Single family home'; and the following words must always be replaced by the word 'Condo': 'Condominium', 'Apartment', 'Flat, 'Penthouse'. Still for the Type, there may be multiple types in the request, in which case assign each of them to a separate field in your response; if there is no type indicated leave that element of your response empty. For the Price, if the price (do not confuse with min/max/range related to the bedrooms or bathrooms) is expressed as a range respond with 'range ' followed by the figures separated by a hyphen, if the price (do not confuse with min/max/range related to the bedrooms or bathrooms) is expressed as a minimum respond with 'minimum ' followed by the figure, if the price (do not confuse with min/max/range related to the bedrooms or bathrooms) is expressed as a maximum (or if the price is just a single figure without further indication) respond with 'maximum ' followed by the figure. Still for the Price, note that the letter 'k' or 'K' after a figure means this figure should be multiplied by 1000; and the letter 'm' or 'M' after a figure means this figure should be multiplied by 1000000. Still for the Price, note that Bedrooms and Bathrooms can also have a minimum, maximum, or range indication, but min/max/range indications related to the Bedrooms or Bathrooms do not apply to the Price therefore make sure to separate indications related to the Pice and indications related to the Bedrooms or Bathrooms and do not take into account any Bedrooms or Bathrooms min/max/range indication in your response about the price. For Features, note that the key features are generally (but not limited to) elements such as: 'Luxury', 'Ocean view', 'Gated community', 'Pool' (if text contains 'Swimming pool' reply 'Pool'), 'Close to the beach', 'Close to schools', 'Beachfront', 'Walking distance to beach', or another specific feature used to described the style, situation, or aspect of the house, but the number of bedrooms or bathrooms or the size of the house must not be considered as special features; note that there may be multiple features in the request, in which case assign each of them to a separate field in your response. Still for the Features, note that a mention such as 'close to LocationA' (for example, 'close to Tamarindo') is not a feature, it is an indication of location. For the Bedrooms, note that the following words must be considered equivalent to 'Bedrooms': 'Beds', 'Br', 'beds', 'br'. Still for Bedrooms, if it's a range (do not confuse with min/max/range related to the price or bathrooms) respond with 'range ' followed by the figures separated by a hyphen, if it's a minimum (do not confuse with min/max/range related to the price or bathrooms) respond with 'minimum ' followed by the figure, if it's a maximum (do not confuse with min/max/range related to the price or bathrooms) respond with 'maximum ' followed by the figure (do not interpret a big number as a maximum if there is no clear indication that it should be considered a maximum), if it's a single figure respond 'exact ' followed by the figure (even if it's a high number with multiple digits, as long as it's just one number). For the Bathrooms, note that the following words must be considered equivalent to 'Bathrooms': 'Baths', 'baths'. Still for Bathrooms, if it's a range respond with 'range ' followed by the figures (separated by a hyphen), if it's a minimum respond with 'minimum ' followed by the figures, if it's a maximum respond with 'maximum ' followed by the figure, if it's a single figure respond 'exact ' followed by the figure. If one of the criteria can't be found in the user prompt, leave one blank space in your response (don't mention 'empty', nor 'undefined', nor 'N/A', leave it blank like this '') but maintain the response format as indicated. Don't include any introduction or explanation.";
        let response2;
        try {
          response2 = await openai2.createChatCompletion({
            model: "gpt-4-1106-preview",
            messages: [
              {
                role: "system",
                content: systemPrompt,
              },
              { role: "user", content: text },
            ],
          });
          // Use response2.data.choices[0].message.content for further operations
        } catch (error) {
          console.error("Error retrieving chat completion: ", error);
          // Handle the error appropriately here
          // You might set a fallback message or handle the error differently depending on your needs
        }

        let AIResponse = response2.data.choices[0].message.content;

        // Check if the AI response is "No search query"
        if (AIResponse.trim() === "No search query") {
          console.log("Received 'No search query' from AI, exiting without replying.");
          return; // Exit the function early without replying to the user
        }

        // Extract the property details from the response
        let propertyDetails = extractDataFromMessage(AIResponse);

        // Check the number of criteria extracted
        let criteriaCount = Object.values(propertyDetails).filter(value => value && value.trim() !== '' && value.trim().toLowerCase() !== 'undefined' && value.trim().toLowerCase() !== 'n/a').length;

        // Print the criteria that have been counted
        console.log("Search criteria : " + Object.values(propertyDetails));

        // If less than 2 criteria were extracted, ask for additional detail and exit the function
        if (criteriaCount < 1) {
          m.reply(`Our apologies, but the details provided were not sufficient to perform a search. Please provide at least 2 criteria among the following: type of property, location, price, number of bedrooms, number of bathrooms, or special features.

To see the userguide, type 'Help'.
To request support, simply start your message with 'Cicero' and explain the situation. We will reply asap.`);
          console.log("No results - Insufficient search criteria");
          return;
        }

        // Query the SQLite database
        const db = new sqlite3.Database('master_db_merged.db', sqlite3.OPEN_READONLY, (err) => {
          if (err) {
            logError(err.message);
            return;
          }
        });

        function searchNearestTown(locationName, callback) {
          const db = new sqlite3.Database('master_db_merged.db', sqlite3.OPEN_READONLY, (err) => {
            if (err) {
              logError(err.message);
              return callback(err);
            }

            const sql = `SELECT * FROM locations WHERE Location_name = ? LIMIT 1`;

            db.get(sql, [locationName.toLowerCase()], (err, row) => {
              if (err) {
                console.error(err.message);
                return callback(err);
              } else {
                if (row) {
                  const nearestTownNamesInRange = Object.keys(row)
                    .filter(key => key.startsWith('Nearest_Town_'))
                    .filter(key => {
                      const rowValue = row[key];
                      if (rowValue) {
                        const distanceArray = rowValue.split(' - ');
                        if (distanceArray.length > 1) {
                          const distance = parseFloat(distanceArray[distanceArray.length - 1]);
                          return !isNaN(distance) && distance >= 0 && distance <= 5;
                        }
                      }
                      return false;
                    })
                    .map(key => row[key].split(' - ')[1]);
                  console.log(nearestTownNamesInRange);
                  callback(null, nearestTownNamesInRange);
                } else {
                  console.log(`No data found for location: ${locationName}`);
                  callback(null, null);
                }
              }

              db.close();
            });
          });
        }

        // Convert the property details into an SQL query
        let sqlQuery = await convertExtractedDataToSQL(propertyDetails, db);

        // Define a function to get the total number of entries
        function getTotalEntries(callback) {
          const countQuery = "SELECT COUNT(*) AS total FROM properties";
          db.get(countQuery, [], (countErr, countRow) => {
            if (countErr) {
              callback(countErr, null); // Pass the error to the callback
            } else {
              callback(null, countRow.total); // Pass the total count to the callback
            }
          });
        }

        // Use the getTotalEntries function
        getTotalEntries((err, totalEntries) => {
          if (err) {
            throw err; // Handle the error appropriately
          }

          console.log(`Total number of entries in the properties database: ${totalEntries}`);


          db.all(sqlQuery, [], (err, initialRows) => {
            if (err) {
              console.error("Error running sql query:", err.message);
              //return;  // Exit the function or handle the error as needed
            }
            console.log("345 - Db query");

            let combinedRows = initialRows.slice();

            processCombinedData();

            async function processCombinedData() {
              console.log("397 - Start processcombineddata");
              let combinedFilteredRows = combinedRows; // Initialize with the original dataset
              /*if(propertyDetails.LocationA === "Guanacaste"){
                  propertyDetails.LocationA = "";
              }*/

              // Collect all location values from propertyDetails that contain 'Location' in the key
              let searchValues = [];
              for (const key in propertyDetails) {
                if (key.startsWith('Location') && propertyDetails[key]) {
                  searchValues.push(propertyDetails[key].toLowerCase());
                  await new Promise((resolve, reject) => {
                    searchNearestTown(propertyDetails[key].toLowerCase(), (err, locations) => {
                      if (err) {
                        reject(err);
                      } else {
                        if (locations && locations.length) {
                          searchValues.push(...locations.filter(x => !x.includes(propertyDetails[key].toLowerCase())))
                        }
                        resolve(searchValues);
                      }
                    });
                  });
                }
              }
              console.log("Search values " + searchValues);
              console.log("Search values length " + searchValues.length)
              console.log("404 - Collect all location values");

              // Check if LocationA is "Guanacaste", apply Location5 filter and then exit to processFilteredData
              if (propertyDetails.LocationA && propertyDetails.LocationA.toLowerCase() === "guanacaste") {
                combinedFilteredRows = combinedFilteredRows.filter(row => row.Location5 && row.Location5.toLowerCase() === "guanacaste");
                console.log("Special filtering for Guanacaste applied, moving to processFilteredData");
                processFilteredData(); // Call processFilteredData directly after applying the filter
                return; // Exit the function
              }

              // Skip location filtering if no location criteria are provided
              //let combinedFilteredRows = combinedRows; // Initialize with the original dataset
              if (searchValues.length > 0) {
                console.log("413 - Start filtering location");
                if (propertyDetails.LocationA != "Guanacaste") {
                  // Define filtering function for Location2_and_aliases
                  const filterByLocation2AndAliases = (row) => {
                    if (!row.Location2_and_aliases) return false;
                    const locations = row.Location2_and_aliases.split('-').map(s => s.trim().toLowerCase());
                    console.log("420 - End filter loc2andal");
                    return searchValues.some(searchValue => locations.some(location => location.includes(searchValue)));
                  };

                  // Apply filter for Location2_and_aliases
                  const filteredRowsLocation2AndAliases = combinedRows.filter(filterByLocation2AndAliases);
                  const nonMatchingRowsLocation2AndAliases = combinedRows.filter(row => !filterByLocation2AndAliases(row));

                  // Define filtering function for Location2
                  const filterByLocation2 = (row) => {
                    if (!row.Location2) return false;
                    console.log("430 - End filter loc2");
                    return searchValues.some(searchValue => row.Location2.toLowerCase().includes(searchValue));
                  };

                  const filteredRowsLocation2 = nonMatchingRowsLocation2AndAliases.filter(filterByLocation2);
                  const nonMatchingRowsLocation2 = nonMatchingRowsLocation2AndAliases.filter(row => !filterByLocation2(row));

                  // Define filtering function for Location1
                  const filterByLocation1 = (row) => {
                    if (!row.Location1) return false;
                    console.log("438 - End filter loc1");
                    return searchValues.some(searchValue => row.Location1.toLowerCase().includes(searchValue));
                  };

                  const filteredRowsLocation1 = nonMatchingRowsLocation2.filter(filterByLocation1);
                  // Combine the results from different filters only if there are location criteria
                  combinedFilteredRows = filteredRowsLocation2AndAliases.concat(filteredRowsLocation2, filteredRowsLocation1);
                  console.log("446 - End filtering");
                }
                // Process the initial rows to find unique duplicate numbers
                let duplicateNumbers = new Set();
                combinedFilteredRows.forEach(row => {
                  if (row.Duplicate) {
                    duplicateNumbers.add(row.Duplicate);
                  }
                });
                console.log("358 - Find unique dup numbers");

                const fetchAndCombineRows = (duplicateNumber, callback) => {
                  let query = `SELECT * FROM properties WHERE Duplicate = ? AND ID NOT IN (${combinedFilteredRows.map(r => r.ID).join(", ")})`;
                  db.all(query, [duplicateNumber], (err, newRows) => {
                    if (err) {
                      callback(err);
                      return;
                    }
                    combinedFilteredRows = combinedFilteredRows.concat(newRows);
                    callback(null); // callback without error
                  });
                };

                // Start fetching and combining rows for each duplicate number
                let tasksCompleted = 0;
                console.log("376 - Start dup rows extraction");
                if (duplicateNumbers.size === 0) {
                  processFilteredData(); // Directly process data if there are no duplicates to fetch
                } else {
                  duplicateNumbers.forEach(duplicateNumber => {
                    fetchAndCombineRows(duplicateNumber, (err) => {
                      if (err) {
                        // Handle error
                        console.error("Error fetching additional rows:", err);
                        return;
                      }

                      tasksCompleted++;
                      if (tasksCompleted === duplicateNumbers.size) {
                        console.log("Task completed");
                        console.log("Dup number size " + duplicateNumbers.size)
                        processFilteredData(); // Process data when all fetching is completed
                      }
                    });
                  });
                  console.log("395 - End dup rows extraction");
                }
              } else {
                processFilteredData();
              }

              function processFilteredData() {
                // Group rows by 'Duplicate' number, treating non-duplicates as individual groups; each group is assigned a most recent date that will serve to sort them
                let groups = {};
                console.log("452 - Group duplicates");
                combinedFilteredRows.forEach(row => {
                  const key = row.Duplicate ? row.Duplicate : `unique_${row.ID}`;
                  if (!groups[key]) {
                    groups[key] = {
                      rows: [],
                      mostRecentDate: new Date('1900-01-01') // Initialize with an old date
                    };
                  }
                  groups[key].rows.push(row);
                  const rowDate = new Date(row.Last_modified_date);
                  if (rowDate > groups[key].mostRecentDate) {
                    groups[key].mostRecentDate = rowDate;
                  }
                });

                // Filter out groups older than 2 years and sort groups by most recent date
                console.log("470 - Filter old groups");
                const currentDate = new Date();
                const twoYearsAgo = new Date(currentDate.setFullYear(currentDate.getFullYear() - 2));
                const sortedGroups = Object.values(groups)
                  .filter(group => group.mostRecentDate > twoYearsAgo)
                  .sort((a, b) => b.mostRecentDate - a.mostRecentDate);

                // Iterate over each group to sort its rows and assign a rank based on recency
                sortedGroups.forEach((group, index) => {
                  // Sort the 'rows' array of each group by 'Last_modified_date' in descending order
                  group.rows.sort((a, b) => {
                    // Convert dates to Date objects for comparison
                    let dateA = new Date(a.Last_modified_date);
                    let dateB = new Date(b.Last_modified_date);
                    return dateB - dateA; // Descending order
                  });

                  // Assigning rank based on the position in the sorted array (1-indexed)
                  group.rank = index + 1;
                });

                // Prepare the result string with a limit of 20 entries
                let results = `Here are the most recent listings I found, out of ${totalEntries} listings from 35 websites in GU & PA:\n`;

                // Start with empty arrays to collect feature and location descriptions
                let typesList = [];
                let locationsList = [];
                let featuresList = [];

                // Loop through each key in propertyDetails to collect types, features, and locations
                console.log("499 - Collect features types locs");
                for (let key in propertyDetails) {
                  // Collect locations
                  if (key.match(/^Location[A-Z]$/)) {
                    locationsList.push(propertyDetails[key]);
                  }
                  // Collect types
                  if (key.match(/^Type[A-Z]$/)) {
                    typesList.push(propertyDetails[key]);
                  }
                  // Collect features
                  if (key.startsWith("Feature")) {
                    featuresList.push(propertyDetails[key]);
                  }
                }

                // Join all collected features and locations into single strings, separated by commas
                let typesDisplay = typesList.join(", ");
                let locationsDisplay = locationsList.join(", ");
                let featuresDisplay = featuresList.join(", ");

                function cleanCriteria(value) {
                  // Check if the value is the string "undefined" and replace it with an empty string
                  return (value === "undefined" || value === undefined) ? "" : value;
                }

                // Sanitize each criteria
                let cleanType = cleanCriteria(typesDisplay);
                let cleanLocations = cleanCriteria(locationsDisplay);
                let cleanPrice = cleanCriteria(propertyDetails.Price);
                let cleanFeatures = cleanCriteria(featuresDisplay);
                let cleanBedrooms = cleanCriteria(propertyDetails.Bedrooms);
                let cleanBathrooms = cleanCriteria(propertyDetails.Bathrooms);

                // Search criteria recap
                results += `_(Type: ${cleanType} - Locations: ${cleanLocations} - Price: ${cleanPrice} - Special features: ${cleanFeatures} - Bedrooms: ${cleanBedrooms} - Bathrooms: ${cleanBathrooms})_\n\n`;
                let displayedResultsIds = [];
                let resultsCount = 0;

                // Iterate through first 20 groups (or fewer, if fewer groups are available)
                console.log("538 - Iterate first 20 groups");


                const getRecencyCategory = (lastModifiedDate) => {
                  if (!lastModifiedDate) return 'NoDate'; // Distinct category for no date

                  const currentDate = new Date();
                  const modifiedDate = new Date(lastModifiedDate.split(".").reverse().join("/"));

                  // Calculate the difference in days
                  const daysDiff = Math.floor((currentDate - modifiedDate) / (1000 * 60 * 60 * 24));

                  // Check for "New" category (14 days or less)
                  if (daysDiff <= 14) return "*New/Updated*";

                  // Calculate the difference in months
                  const monthsDiff = currentDate.getMonth() - modifiedDate.getMonth() + (12 * (currentDate.getFullYear() - modifiedDate.getFullYear()));

                  if (monthsDiff <= 6) return "<6mths";
                  if (monthsDiff <= 12) return "<1yr";
                  if (monthsDiff <= 24) return "<2yrs";
                  return 'Older'; // Distinct category for more than 2 years old
                  console.log("564 - End getrecency");
                };

                const formatPrice = (price) => {
                  if (!price) return ''; // Return an empty string if the price is not available
                  return `$${parseFloat(price).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
                };
                sortedGroups.slice(0, 20).forEach(group => {
                  if (resultsCount >= 20) {
                    return;
                  }
                  const addResult = (header, type, price, url, date, locationAliases) => {
                    locationAliases = locationAliases || '';
                    let firstAlias = locationAliases ? locationAliases.split(' - ')[0] : '';
                    if (firstAlias) {
                      firstAlias = firstAlias.split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
                    }
                    results += '*' + header + '*' + ' - ';
                    results += type + ' - ';
                    if (firstAlias) {
                      results += firstAlias + ' - ';
                    }
                    results += formatPrice(price) + '\n' + url;
                    const recencyCategory = getRecencyCategory(date);
                    if (recencyCategory !== 'NoDate') {
                      results += '\n' + '_' + recencyCategory + '_';
                    }
                    results += '\n\n';
                  };

                  const addEntryResult = (entry) => {
                    let recencyCategory = getRecencyCategory(entry.Last_modified_date);
                    if (recencyCategory !== 'Older') {
                      let header = entry.Header.split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
                      let price = entry.Price;
                      let url = entry.Original_URL.replace(/["']/g, "");
                      addResult(header, entry.Type, price, url, entry.Last_modified_date, entry.Location2_and_aliases);
                      displayedResultsIds.push(entry.ID);
                      resultsCount++;
                    }
                  };

                  const addGroupResult = (group) => {
                    let mostRecentEntry = group.rows[0];
                    let recencyCategory = getRecencyCategory(mostRecentEntry.Last_modified_date);
                    if (recencyCategory !== 'Older') {
                      let header = mostRecentEntry.Header.split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
                      let price = mostRecentEntry.Price;
                      let firstAlias = mostRecentEntry.Location2_and_aliases ? mostRecentEntry.Location2_and_aliases.split(' - ')[0] : '';
                      firstAlias = firstAlias.split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
                      results += '*' + header + '*' + ' - ';
                      results += mostRecentEntry.Type + ' - ';
                      if (firstAlias) {
                        results += firstAlias + ' - ';
                      }
                      results += formatPrice(price) + '\n';
                      let urlsAdded = new Set();
                      group.rows.forEach(row => {
                        let url = row.Original_URL.replace(/["']/g, "");
                        if (!urlsAdded.has(url)) {
                          results += ' - ' + url + '\n';
                          urlsAdded.add(url);
                        }
                      });
                      results += '_' + recencyCategory + '_\n\n';
                      displayedResultsIds.push(mostRecentEntry.ID);
                      resultsCount++;
                    }
                  };

                  if (group.rows.length === 1) {
                    addEntryResult(group.rows[0]);
                  } else {
                    addGroupResult(group);
                  }
                });

                // Add a line if the maximum number of unique results (20) is reached
                if (resultsCount >= 20) {
                  results += "\nMaximum number of unique results per search (20) reached.\n";
                }

                results += "\n_Type 'help' to see the user guide or, for support, send us a message starting with 'Cicero' followed by your request and we will reply asap._";
                results += "\n\nEmperia Technologies SRL | Potrero, GU, Costa Rica | All rights reserved.";

                // Log the final results string before sending the reply
                console.log("Reply to user: " + results);

                // Check if adding these results exceeds the user's limit
                if (senderNumber !== "50661209372" && !updateAndCheckUserResults(senderNumber, displayedResultsIds)) {
                  m.reply("You have reached the daily limit of 80 new, unique results. Please try again tomorrow.");
                  return;
                }

                // REPLY TO THE USER
                if (resultsCount === 0) {
                  let typesList = [];
                  let featuresList = [];
                  let locationsList = [];
                  // Loop through each key in propertyDetails to collect features and locations
                  for (let key in propertyDetails) {
                    // Collect locations
                    if (key.match(/^Location[A-Z]$/)) {
                      locationsList.push(propertyDetails[key]);
                    }
                    // Collect types
                    if (key.match(/^Type[A-Z]$/)) {
                      typesList.push(propertyDetails[key]);
                    }
                    // Collect features
                    if (key.startsWith("Feature")) {
                      featuresList.push(propertyDetails[key]);
                    }
                  }

                  // Join all collected features and locations into single strings, separated by commas
                  let typesDisplay = typesList.join(", ");
                  let featuresDisplay = featuresList.join(", ");
                  let locationsDisplay = locationsList.join(", ");

                  m.reply(`My apologies, I couldn't find any matching property. Could you kindly widen or modify the criteria? \n _(Type: ${typesDisplay} - Location: ${locationsDisplay} - Price: ${propertyDetails.Price} - Special features: ${featuresDisplay} - Bedrooms: ${propertyDetails.Bedrooms})_ \n\nType 'help' at anytime to see the user guide.`);
                  console.log("No results - No matching property");
                } else {
                  // Reply with the results
                  m.reply(results);
                }
              }

              db.close((err) => {
                if (err) {
                  logError(err.message);
                }
              });
            }
          });
        }); // End gettotalentries
      } catch (error) {
        if (error.response) {
          const errorMessage = `${error.response.status}\n\n${error.response.data}`;
          logError(errorMessage);
        } else {
          logError(error.toString());
          //m.reply("Sorry, there seems to be an error: " + error.message);
        }
      }
      return;
    }

    // Function to extract data from message
    function extractDataFromMessage(message) {
      // Split the message by commas and spaces to get key-value pairs
      const data = message.split(", ");
      console.log(`Sender: ${senderNumber}`);
      console.log(`Message: ${body}`);  // Log the initial message

      // Initialize an object to hold the extracted data
      const extractedData = {};

      // Loop through each key-value pair
      data.forEach(pair => {

        // Split the pair by the colon to get the key and value
        let [key, value] = pair.split(": ");

        // Remove any trailing colons from the key
        key = key.replace(/:$/, '');

        // Add the key-value pair to the extractedData object
        extractedData[key] = value;

        // Print each key and data pair
        console.log(`Extracted - Key: ${key}, Value: ${value}`);
      });

      // Return the extracted data
      return extractedData;
    }

    // Function to convert extracted data to SQL query
    function convertExtractedDataToSQL(data) {
      const trimLowerCase = (str) => str.trim().toLowerCase();

      const featureRegex = /^Feature\d+$/;
      const typeRegex = /^Type[A-Z]$/;
      const emptyPattern = /AND\s+TRIM\(\w+\)\s+LIKE\s+'%(empty|'')%'/g;

      const sqlParts = [
        "SELECT ID, Header, Original_URL, Location1, Location2, Location2_and_aliases, Location5, Type, Price, Last_modified_date, Duplicate FROM properties WHERE"
      ];

      const featureFieldMap = {
        "Beachfront": "Beachfront",
        "Gated community": "Gated_community",
        "Ocean view": "Ocean_view",
        "Close to the beach": "Close_to_beach",
        "Walking distance to beach": "Walk_to_beach",
        "Close to schools": "Close_to_schools",
        "Luxury": "Luxury",
        "Pool": "Swimming_pool"
      };

      let excludedWords = ["de", "del", "la", "las", "el", "los"];

      let typeValues = [];
      let otherConditions = [];

      const validKeys = Object.keys(data).filter(key =>
        data[key] &&
        trimLowerCase(data[key]) !== '' &&
        trimLowerCase(data[key]) !== 'undefined' &&
        trimLowerCase(data[key]) !== 'n/a' &&
        !key.includes('Location')
      );
      console.log("validKeys====>", validKeys);

      validKeys.forEach(key => {
        key = key.trim();
        const value = data[key]?.trim();

        if (typeRegex.test(key) && data[key]) {
          typeValues.push(`'${data[key]}'`);
        } else {
          let condition = "";

          const handlePrice = () => {
            if (!data[key]) return;
            let [classifier, figures] = data[key].split(/ (.+)/);
            classifier = trimLowerCase(classifier);

            if (classifier === "range") {
              let rangeParts = figures.split(/-|\s/).map(part => parseFigures(part));
              if (rangeParts.length === 2 && !isNaN(rangeParts[0]) && !isNaN(rangeParts[1])) {
                condition = `(Price BETWEEN ${rangeParts[0]} AND ${rangeParts[1]}) `;
                otherConditions.push(condition);
              }
            } else {
              const parsedFigures = parseFigures(figures);
              switch (classifier) {
                case "minimum":
                  if (!isNaN(parsedFigures)) {
                    condition = `(Price >= ${parsedFigures})`;
                    otherConditions.push(condition);
                  }
                  break;
                case "maximum":
                  if (!isNaN(parsedFigures)) {
                    condition = `(Price <= ${parsedFigures})`;
                    otherConditions.push(condition);
                  }
                  break;
                default:
                  console.log(`Unexpected price classifier: ${classifier}`);
              }
            }
          };

          const handleBedroomsOrBathrooms = (field) => {
            if (!data[key]) return;
            let [classifier, figures] = data[key].split(/ (.+)/);
            classifier = classifier.toLowerCase();

            switch (classifier) {
              case "range":
                let rangeParts = figures.split(/-|\s/).map(part => parseFloat(part.trim()));
                if (rangeParts.length === 2 && !isNaN(rangeParts[0]) && !isNaN(rangeParts[1])) {
                  condition = `(${field} BETWEEN ${rangeParts[0]} AND ${rangeParts[1]}) `;
                  otherConditions.push(condition);
                }
                break;
              case "minimum":
                if (!isNaN(figures)) {
                  condition = `(${field} >= ${figures}) `;
                  otherConditions.push(condition);
                }
                break;
              case "maximum":
                if (!isNaN(figures)) {
                  condition = `(${field} <= ${figures}) `;
                  otherConditions.push(condition);
                }
                break;
              case "exact":
                if (!isNaN(figures)) {
                  condition = `(${field} = ${figures}) `;
                  otherConditions.push(condition);
                }
                break;
              default:
                console.log(`Unexpected ${field} classifier: ${classifier}`);
            }
          };

          const handleLotSize = () => {
            if (!data[key] || isNaN(data[key])) return;
            let lowerBound = data[key] * 0.9;
            let upperBound = data[key] * 1.1;
            condition = `(Size_lot_sqm >= ${lowerBound} AND Size_lot_sqm <= ${upperBound}) `;
            otherConditions.push(condition);
          };

          const parseFigures = (figures) => {
            const multiplier = figures.toLowerCase().endsWith('k') ? 1000 : figures.toLowerCase().endsWith('m') ? 1000000 : 1;
            return parseFloat(figures.replace(/[km\s]/gi, '')) * multiplier;
          };

          if (key === "Price") {
            handlePrice();
          } else if (key === "Bedrooms") {
            handleBedroomsOrBathrooms("Bedrooms");
          } else if (key === "Bathrooms") {
            handleBedroomsOrBathrooms("Bathrooms");
          } else if (key === "Lot size") {
            handleLotSize();
          } else if (featureRegex.test(key)) {
            let featureValue = data[key];
            if (featureValue in featureFieldMap) {
              condition = `${featureFieldMap[featureValue]} = 'Yes' `;
              otherConditions.push(condition);
            } else {
              data[key] += "/not supported";
            }
          } else {
            sqlQuery += `${key} = '${data[key]}' `;
          }
        }
      });

      const conditions = otherConditions.length > 0 ? otherConditions.join(" AND ") : null;

      if (typeValues.length > 0 || conditions) {
        const typeCondition = typeValues.length > 0 ? `Type IN (${typeValues.join(", ")})` : "";
        const combinedCondition = [typeCondition, conditions].filter(Boolean).join(" AND ");
        sqlParts.push(combinedCondition);
      }

      let sqlQuery = sqlParts.join(" ");
      sqlQuery = sqlQuery.trim();

      if (sqlQuery.endsWith("WHERE")) {
        sqlQuery = sqlQuery.substring(0, sqlQuery.lastIndexOf("WHERE")).trim();
      }

      if (sqlQuery.endsWith("AND")) {
        sqlQuery = sqlQuery.substring(0, sqlQuery.lastIndexOf("AND")).trim();
      }

      let matches = sqlQuery.match(emptyPattern);
      if (matches) {
        matches.forEach(match => {
          console.log(`The SQL query contains an invalid '${match}' parameter.`);
          sqlQuery = sqlQuery.replace(match, "");
        });
      }

      while (sqlQuery.includes('AND AND') || sqlQuery.includes('OR OR')) {
        sqlQuery = sqlQuery.replace(/AND\s+AND/g, "AND").replace(/OR\s+OR/g, "OR");
      }

      sqlQuery = sqlQuery.replace(/WHERE\s+(AND|OR)/, "WHERE");

      console.log(`Final query ${sqlQuery}`);
      return sqlQuery;
    }

    // Function to update and check the user's results count
    function updateAndCheckUserResults(senderNumber, displayedResultsIds) {
      const currentDate = getCurrentDate();

      if (!userResultsData[senderNumber] || userResultsData[senderNumber].date !== currentDate) {
        userResultsData[senderNumber] = { ids: new Set(), count: 0, date: currentDate };
      }

      let newResultsCount = 0;
      displayedResultsIds.forEach(id => {
        if (!userResultsData[senderNumber].ids.has(id)) {
          userResultsData[senderNumber].ids.add(id);
          newResultsCount += 1;
        }
      });

      userResultsData[senderNumber].count += newResultsCount;
      console.log(`New results count for ${senderNumber}: ${newResultsCount}`);
      console.log(`Total results count for ${senderNumber}: ${userResultsData[senderNumber].count}`)
      return userResultsData[senderNumber].count <= 80;
    }


    switch (command) {
      case "help":
        // Logging the /help command and sender details
        console.log(`[ LOGS ] Command: help | From: ${pushname} [ ${sender.replace("@s.whatsapp.net", "")} ]`);

        m.reply(`*Welcome to Cicero (beta version) - Fast, simple, and clear property search in Costa Rica.*

*How to search for properties*.
Simply ask what you're looking for, for example:
"a house with min 3 bedrooms in Potrero, max 600k"
"3 br 3 baths ocean view house with pool, close to schools, Playas del Coco, <900k"
"I'm looking for a luxury villa in Flamingo, 4beds, 1.2-1.5m"
(the .s prefix is no longer needed to perform a search)

*How to read the results*
Individual results:
'*Luxury ocean view villa in Playa Panama* - _Header_
  https://aaa.luxrealestate.cr/luxury-villa - _Link to the listing page_
  _6mths_' - _Estimation of how recent the listing is (New = <2 wks)_

Grouped results (duplicates listed on different websites):
'*Luxury ocean view villa in Playa Panama* - _Header_
  - https://aaa.luxrealestateco.cr/luxury-villa - _Link to the listing page_
  - https://aaa.bestrealestateco.cr/villa-playa-panama - _Link to the listing page_
  - https://aaa.toprealestateco.cr/ocean-view-villa - _Link to the listing page_
  _6mths_' - _Estimation of how recent the most recent of the duplicate listing is_

*What you can search for and where*
Search criteria:
- Location
- Price
- Number of bedrooms
- Number of bathrooms
- Features (the following are supported: ocean view, pool, gated community, close to the beach, walk to beach, beachfront, close to schools, luxury)

Results in this beta version includes only:
- Single-family homes and condos
- In Guanacaste and part of the Puntarenas province
_-> Let us know what else you would like to be able to search for!_

*How results are sorted*
Results are sorted by most recently active.
A maximum of 20 results per query, and 80 total new unique results (the same listing appearing in multiple results list is counted only once) per day are displayed,.
Listings that are estimated to be >2 years do not appear.
Duplicate listings are grouped (not 100% accurate yet).

*How to get contact us*
In case you need direct support, simply *send a message starting with "Cicero"* (for example: "Cicero, I have a question about...") and we will reply as soon as possible.

*Who we are*
Cicero is a product from Emperia Technologies S.R.L., a company registered in the "Registro Nacional - República de Costa Rica" under registration number 3-102-889847-1 and based in Potrero, Guanacaste.  
`)
        // Logging the help message sent
        console.log(`Help message sent to ${pushname} [ ${sender.replace("@s.whatsapp.net", "")} ]`);
        break;

      case "Cicero": case "cicero":
        // Logging the Cicero command and sender details
        console.log(`[ LOGS ] Command: Cicero | From: ${pushname} [ ${sender.replace("@s.whatsapp.net", "")} ]`);

        m.reply(`Thank you for your message, we will get back to you as soon as possible.
            
Cicero`)
        // Logging the help message sent
        console.log(`Cicero msg reception confirmation sent to ${pushname} [ ${sender.replace("@s.whatsapp.net", "")} ]`);
        break;



      case "ai": case "openai":
        try {
          if (setting.keyopenai === "ISI_APIKEY_OPENAI_DISINI")
            return reply("Apikey belum diisi\n\nSilahkan isi terlebih dahulu apikeynya di file key.json\n\nApikeynya bisa dibuat di website: https://beta.openai.com/account/api-keys");

          if (!text) return reply(`Chat dengan AI.\n\nContoh:\n${prefix}${command} Apa itu resesi`);

          const configuration = new Configuration({
            apiKey: setting.keyopenai,
          });

          const openai = new OpenAIApi(configuration);

          const response1 = await openai.createChatCompletion({
            model: "gpt-4-1106-preview",
            messages: [
              {
                role: "system",
                content:
                  "You act as a real estate property research assistant. Your only job is to help the user find a house in Guanacaste. You want the user to provide you with a location, a number of bedrooms, or a price point or a price range. If the request from the user doesn't relate to real estate, invite the user to reframe their request",
              },
              { role: "user", content: text },
            ],
          });

          m.reply(`${response1.data.choices[0].message.content}`);

        } catch (error) {
          if (error.response) {
            const errorMessage = `${error.response.status}\n\n${error.response.data}`;
            logError(errorMessage);
          } else {
            logError(error.toString());
            m.reply("There was an error: " + error.message);
          }
        }
        break;
    }  // end of switch statement

  } catch (e) {  // Here catch the error and log it.
    console.log('Error:', JSON.stringify(e, Object.getOwnPropertyNames(e), 2));
  }
}  // end of sansekai function