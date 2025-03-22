// main.js - Core functionality for Twitter Sentiment Analysis UI

// API endpoints
const API_ENDPOINTS = {
    STATUS: '/twitter/status',
    MANUAL_TWEET: '/twitter/manual-tweets',
    COLLECT: '/twitter/collect',
    TOKENS: '/twitter/tokens',
    NETWORKS: '/twitter/networks'
};

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
        const tweetData = {
            influencer_username: formData.get('influencer_username'),
            text: formData.get('tweet_text'),
            retweet_count: parseInt(formData.get('retweet_count') || '0'),
            like_count: parseInt(formData.get('like_count') || '0')
        };

        // Add created_at if provided
        const createdAt = formData.get('created_at');
        if (createdAt) {
            tweetData.created_at = new Date(createdAt).toISOString();
        }

        // Submit tweet
        const result = await apiRequest(API_ENDPOINTS.MANUAL_TWEET, {
            method: 'POST',
            body: JSON.stringify(tweetData)
        });

        // Show success message
        statusElement.innerHTML = `
            <div class="alert alert-success">
                Tweet processed successfully!
            </div>
            <div class="tweet-card p-3 border bg-light">
                <div class="d-flex justify-content-between">
                    <strong>@${result.author_username}</strong>
                    <small>${new Date(result.created_at).toLocaleString()}</small>
                </div>
                <p>${formatTweetText(result.text)}</p>
                <div class="text-muted">
                    <small>ID: ${result.tweet_id}</small> |
                    <span>♻️ ${result.retweet_count}</span> |
                    <span>❤️ ${result.like_count}</span>
                </div>
            </div>
        `;

        // Display token mentions if available
        if (result.token_mentions && result.token_mentions.length > 0) {
            const mentionsPanel = document.getElementById('token-mentions-panel');
            const mentionsList = document.getElementById('token-mentions-list');

            mentionsPanel.classList.remove('d-none');
            mentionsList.innerHTML = '';

            result.token_mentions.forEach(mention => {
                const li = document.createElement('li');
                li.className = 'list-group-item';
                li.textContent = mention;
                mentionsList.appendChild(li);
            });
        }

        // Add to activity log if we're on the dashboard
        addActivityLog(`Manual tweet added: ${tweetData.text.substring(0, 30)}...`, 'success');
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
 * Format tweet text with highlighted cashtags and hashtags
 * @param {string} text - Raw tweet text
 * @returns {string} - Formatted HTML
 */
function formatTweetText(text) {
    // Highlight cashtags
    let formatted = text.replace(/\$([A-Za-z0-9]+)/g, '<span class="cashtag">$$$1</span>');

    // Highlight hashtags
    formatted = formatted.replace(/#([A-Za-z0-9_]+)/g, '<span class="hashtag">#$1</span>');

    return formatted;
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

/**
 * Update Kafka monitoring
 * This is a placeholder function for the monitoring page
 */
function updateKafkaMonitoring() {
    // В реална имплементация, това би извикало API за получаване на данни за мониторинг
    console.log('Kafka monitoring update triggered');

    // Актуализирай статуса на връзката
    const statusEl = document.getElementById('kafka-connection-status');
    if (statusEl) {
        statusEl.textContent = 'Connected';
        statusEl.className = 'badge bg-success';
    }

    // Актуализирай списъка с топици
    const topicsEl = document.getElementById('kafka-topics');
    if (topicsEl) {
        topicsEl.innerHTML = `
            <li class="list-group-item d-flex justify-content-between align-items-center">
                twitter-raw-tweets
                <span class="badge bg-primary rounded-pill">3 partitions</span>
            </li>
            <li class="list-group-item d-flex justify-content-between align-items-center">
                token-mentions
                <span class="badge bg-primary rounded-pill">3 partitions</span>
            </li>
            <li class="list-group-item d-flex justify-content-between align-items-center">
                sentiment-results
                <span class="badge bg-primary rounded-pill">3 partitions</span>
            </li>
        `;
    }

    // Актуализирай списъка с консуматори
    const consumersEl = document.getElementById('kafka-consumers');
    if (consumersEl) {
        consumersEl.innerHTML = `
            <li class="list-group-item d-flex justify-content-between align-items-center">
                tweet-processor
                <span class="badge bg-success rounded-pill">Active</span>
            </li>
            <li class="list-group-item d-flex justify-content-between align-items-center">
                token-processor
                <span class="badge bg-success rounded-pill">Active</span>
            </li>
            <li class="list-group-item d-flex justify-content-between align-items-center">
                sentiment-analyzer
                <span class="badge bg-success rounded-pill">Active</span>
            </li>
        `;
    }

    // Актуализирай таблицата с последни съобщения
    const recentMsgsEl = document.getElementById('recent-messages');
    if (recentMsgsEl && recentMsgsEl.querySelector('tbody')) {
        recentMsgsEl.querySelector('tbody').innerHTML = `
            <tr>
                <td>twitter-raw-tweets</td>
                <td>${new Date().toLocaleTimeString()}</td>
                <td>Processed tweet from @test_user about $SOL</td>
            </tr>
            <tr>
                <td>token-mentions</td>
                <td>${new Date().toLocaleTimeString()}</td>
                <td>Detected token $SOL in tweet</td>
            </tr>
            <tr>
                <td>sentiment-results</td>
                <td>${new Date().toLocaleTimeString()}</td>
                <td>Analyzed sentiment: POSITIVE (0.78)</td>
            </tr>
        `;
    }

    // В реална имплементация бихме добавили данни от истинския Kafka
    addActivityLog('Monitoring updated', 'info');
}
