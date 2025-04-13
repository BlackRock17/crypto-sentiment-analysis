// main.js - Core functionality for Twitter Sentiment Analysis UI

// SSE variables
let eventSource = null;
let currentProcessingId = null;

// API endpoints
const API_ENDPOINTS = {
    STATUS: '/twitter/status',
    TWEETS: '/twitter/tweets',
    TWEET_EVENTS: '/twitter/tweets/events'
};

/**
 * Connect to SSE for real-time updates
 */
function connectToSSE() {
    if (eventSource !== null) {
        return; // Already connected
    }

    eventSource = new EventSource(API_ENDPOINTS.TWEET_EVENTS);

    eventSource.addEventListener('connected', function(event) {
        const data = JSON.parse(event.data);
        console.log("SSE connected with queue ID:", data.queue_id);
    });

    eventSource.addEventListener('tweet_processed', function(event) {
        const data = JSON.parse(event.data);
        console.log("Tweet processed event:", data);
        console.log("Tweet operation:", data.operation);  // Дебъг лог за операцията

        // Check if this is for our current tweet
        if (data.processing_id === currentProcessingId) {
            const statusElement = document.getElementById('processing-status');
            if (!statusElement) return;

            if (data.status === "success") {
                // Получаване на текста за визуализация от формата или от съхранения текст
                const tweetText = document.getElementById('tweet_text') ?
                                  document.getElementById('tweet_text').value :
                                  sessionStorage.getItem('lastTweetText') || '';

                // Внимателно проверяваме типа на операцията
                let operationMsg, alertClass;

                if (data.operation === "created") {
                    operationMsg = "Tweet created successfully";
                    alertClass = "alert-success";
                    console.log("Showing created message");
                } else if (data.operation === "existing") {
                    operationMsg = "Found existing tweet";
                    alertClass = "alert-warning";
                    console.log("Showing existing message");
                } else {
                    // Резервен вариант, ако статусът е неочакван
                    operationMsg = "Tweet processed";
                    alertClass = "alert-info";
                    console.log("Showing generic message for operation:", data.operation);
                }

                statusElement.innerHTML = `
                    <div class="alert ${alertClass}">
                        ${operationMsg}! Tweet ID: ${data.tweet_id}, Database ID: ${data.db_id}
                    </div>
                    <div class="card p-3 border bg-light">
                        <p>${tweetText}</p>
                        <small class="text-muted">ID: ${data.tweet_id}</small>
                    </div>
                `;

                // Add to activity log
                addActivityLog(operationMsg, data.operation === "created" ? 'success' : 'info');
            } else {
                statusElement.innerHTML = `
                    <div class="alert alert-danger">
                        Tweet processing failed: ${data.message}
                    </div>
                `;
                // Add to activity log
                addActivityLog(`Tweet processing failed`, 'error');
            }

            // Reset current processing ID
            currentProcessingId = null;
        }
    });

    eventSource.addEventListener('heartbeat', function(event) {
        // Heartbeat event - connection is alive
        console.log("SSE heartbeat received");
    });

    eventSource.addEventListener('error', function(event) {
        console.error("SSE connection error:", event);

        // Try to reconnect (browser will usually do this automatically)
        setTimeout(() => {
            if (eventSource) {
                eventSource.close();
                eventSource = null;
                connectToSSE();
            }
        }, 3000);
    });
}

/**
 * Make API request
 * @param {string} url - API endpoint
 * @param {Object} options - fetch options
 * @returns {Promise} - fetch promise
 */
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    };

    // Merge options
    const mergedOptions = { ...defaultOptions, ...options };

    try {
        const response = await fetch(url, mergedOptions);

        // Handle errors
        if (!response.ok) {
            const errorData = await response.json().catch(() => null);
            throw new Error(errorData?.detail || `API error: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API request failed:', error);
        addActivityLog(`API error: ${error.message}`, 'error');
        throw error;
    }
}

/**
 * Update dashboard statistics
 */
async function updateDashboardStats() {
    try {
        const statusElement = document.getElementById('kafka-status');
        const tweetsElement = document.getElementById('tweets-processed');
        const mentionsElement = document.getElementById('mentions-found');

        if (!statusElement) return; // Not on dashboard page

        // Show loading state
        statusElement.innerHTML = '<div class="spinner"></div>';

        // Get status data
        try {
            const statusData = await apiRequest(API_ENDPOINTS.STATUS);

            if (!statusData) return;

            // Update UI
            tweetsElement.textContent = statusData.stored_tweets.toLocaleString();
            mentionsElement.textContent = statusData.token_mentions.toLocaleString();

            // Update Kafka status with appropriate color
            const statusBadge = statusData.twitter_connection === 'ok' ?
                '<span class="badge bg-success">Connected</span>' :
                '<span class="badge bg-danger">Error</span>';
            statusElement.innerHTML = statusBadge;

            // Add to activity log
            addActivityLog('Dashboard stats updated', 'info');
        } catch (error) {
            console.error('Error fetching status:', error);
            statusElement.innerHTML = '<span class="badge bg-warning">Unknown</span>';
        }
    } catch (error) {
        console.error('Failed to update dashboard stats:', error);
    }
}

/**
 * Check Kafka connection status
 */
async function checkKafkaStatus() {
    try {
        // Show loading
        const statusElement = document.getElementById('kafka-status');
        statusElement.innerHTML = '<div class="spinner"></div>';

        // Get status
        try {
            const statusData = await apiRequest(API_ENDPOINTS.STATUS);

            // Update UI based on status
            const statusBadge = statusData.twitter_connection === 'ok' ?
                '<span class="badge bg-success">Connected</span>' :
                '<span class="badge bg-danger">Error</span>';

            statusElement.innerHTML = statusBadge;

            // Add to activity log
            addActivityLog(`Kafka status: ${statusData.twitter_connection}`,
                statusData.twitter_connection === 'ok' ? 'success' : 'error');
        } catch (error) {
            console.error('Error checking status:', error);
            statusElement.innerHTML = '<span class="badge bg-warning">Check Failed</span>';
        }
    } catch (error) {
        console.error('Failed to check Kafka status:', error);
    }
}

/**
 * Submit a manual tweet
 * @param {Event} event - Form submission event
 */
async function submitTweet(event) {
    event.preventDefault();
    console.log("Submit tweet function called");

    // Get form and processing status element
    const form = event.target;
    const statusElement = document.getElementById('processing-status');

    // Show loading
    statusElement.innerHTML = `
        <div class="alert alert-info">
            <div class="spinner"></div> Processing tweet...
        </div>
    `;

    try {
        // Get form data
        const formData = new FormData(form);

        // Get tweet text and generate a unique ID if not provided
        const tweetText = formData.get('tweet_text') || '';
        const tweetId = formData.get('tweet_id') || `manual_${Date.now()}`;

        // Get timestamp or use current time
        let createdAt = formData.get('created_at');
        if (!createdAt) {
            const now = new Date();
            createdAt = now.toISOString();
        }

        const tweetData = {
            tweet_id: tweetId,
            text: tweetText,
            created_at: createdAt
        };

        // Сохраняем текст твита на случай, если форма будет очищена
        sessionStorage.setItem('lastTweetText', tweetText);

        console.log("Sending data to API:", tweetData);
        const result = await apiRequest(API_ENDPOINTS.TWEETS, {
            method: 'POST',
            body: JSON.stringify(tweetData)
        });

        console.log("API response:", result);

        if (result.status === "processing") {
            // Store the processing ID to match with SSE events
            currentProcessingId = result.processing_id;

            statusElement.innerHTML = `
                <div class="alert alert-info">
                    <div class="spinner"></div> Tweet sent to Kafka, waiting for processing...
                </div>
                <div class="card p-3 border bg-light">
                    <p>${tweetText}</p>
                    <small class="text-muted">ID: ${tweetId}</small>
                </div>
            `;

            // Set a timeout in case processing takes too long
            setTimeout(() => {
                if (currentProcessingId === result.processing_id) {
                    // Проверяваме дали туитът е обработен чрез API повикване
                    apiRequest(`${API_ENDPOINTS.TWEETS}/status/${tweetId}`)
                        .then(statusResult => {
                            if (statusResult.status === "success") {
                                // Показваме успешен резултат, но проверяваме типа операция
                                let operationMsg, alertClass;

                                console.log("Status check response:", statusResult);
                                console.log("Operation type:", statusResult.operation);

                                if (statusResult.operation === "created") {
                                    operationMsg = "Tweet created successfully";
                                    alertClass = "alert-success";
                                } else if (statusResult.operation === "existing") {
                                    operationMsg = "Found existing tweet";
                                    alertClass = "alert-warning";
                                } else {
                                    operationMsg = "Tweet processed";
                                    alertClass = "alert-info";
                                }

                                statusElement.innerHTML = `
                                    <div class="alert ${alertClass}">
                                        ${operationMsg}! Tweet ID: ${statusResult.tweet_id}, Database ID: ${statusResult.db_id}
                                    </div>
                                    <div class="card p-3 border bg-light">
                                        <p>${tweetText}</p>
                                        <small class="text-muted">ID: ${tweetId}</small>
                                    </div>
                                `;
                                addActivityLog(operationMsg, statusResult.operation === "created" ? 'success' : 'info');
                            } else {
                                // Показваме timeout съобщение
                                statusElement.innerHTML = `
                                    <div class="alert alert-warning">
                                        Tweet processing is taking longer than expected. It may still be processed in the background.
                                    </div>
                                    <div class="card p-3 border bg-light">
                                        <p>${tweetText}</p>
                                        <small class="text-muted">ID: ${tweetId}</small>
                                    </div>
                                `;
                                addActivityLog(`Tweet processing timeout`, 'warning');
                            }

                            // Reset current processing ID
                            currentProcessingId = null;
                        })
                        .catch(error => {
                            console.error("Error checking tweet status:", error);
                            // Показваме timeout съобщение при грешка
                            statusElement.innerHTML = `
                                <div class="alert alert-warning">
                                    Tweet processing is taking longer than expected. It may still be processed in the background.
                                </div>
                                <div class="card p-3 border bg-light">
                                    <p>${tweetText}</p>
                                    <small class="text-muted">ID: ${tweetId}</small>
                                </div>
                        `;
                        currentProcessingId = null;
                    });
                }
            }, 5000); // Проверка след 5 секунди
        } else {
            // В случай, че имаме директен отговор (не чрез SSE)
            statusElement.innerHTML = `
                <div class="alert alert-success">
                    Tweet submitted successfully! ID: ${result.tweet_id}
                </div>
                <div class="card p-3 border bg-light">
                    <p>${tweetText}</p>
                    <small class="text-muted">ID: ${tweetId}</small>
                </div>
            `;
            // Add to activity log
            addActivityLog(`Tweet submitted successfully`, 'success');
        }

        // Clear form
        form.reset();

        // Set current time in the form
        const nowInput = new Date();
        nowInput.setMinutes(nowInput.getMinutes() - nowInput.getTimezoneOffset());
        document.getElementById('created_at').value = nowInput.toISOString().slice(0, 16);

    } catch (error) {
        console.error('Failed to submit tweet:', error);

        // Show error message
        statusElement.innerHTML = `
            <div class="alert alert-danger">
                Failed to process tweet: ${error.message}
            </div>
        `;
    }
}

/**
 * Add entry to activity log
 * @param {string} message - Log message
 * @param {string} type - Log type (info, success, warning, error)
 */
function addActivityLog(message, type = 'info') {
    const logElement = document.getElementById('activity-log');
    if (!logElement) return; // Not on a page with the log

    // Remove "no activity" placeholder if present
    const placeholder = logElement.querySelector('.text-muted');
    if (placeholder) {
        placeholder.remove();
    }

    // Create log entry
    const entry = document.createElement('li');
    entry.className = `list-group-item log-entry ${type}`;

    // Format time
    const time = new Date().toLocaleTimeString();

    // Set content
    entry.innerHTML = `
        <div class="d-flex justify-content-between align-items-start">
            <span>${message}</span>
            <span class="log-timestamp">${time}</span>
        </div>
    `;

    // Add to log (at the beginning)
    logElement.insertBefore(entry, logElement.firstChild);

    // Limit number of entries
    const maxEntries = 20;
    while (logElement.children.length > maxEntries) {
        logElement.removeChild(logElement.lastChild);
    }
}

// Initialize connection when page loads
document.addEventListener('DOMContentLoaded', function() {
    // Initialize SSE connection
    connectToSSE();

    // Initialize dashboard if on dashboard page
    updateDashboardStats();

    // Setup button handlers
    const checkStatusBtn = document.getElementById('check-status-btn');
    if (checkStatusBtn) {
        checkStatusBtn.addEventListener('click', checkKafkaStatus);
    }

    // Setup form submission
    const tweetForm = document.getElementById('tweet-form');
    if (tweetForm) {
        tweetForm.addEventListener('submit', submitTweet);

        // Default the date/time field to current time
        const now = new Date();
        now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
        const dateTimeField = document.getElementById('created_at');
        if (dateTimeField) {
            dateTimeField.value = now.toISOString().slice(0, 16);
        }
    }
});
