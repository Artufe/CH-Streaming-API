# Companies House Streaming Data
Companies House offers a relatively new service called "Streaming API"<br>
This feature offers a real-time feed of data pertaining to updates of data within Companies House<br>

<b>The aim of this project is:</b> <br>
1. Gather all the streams and store the data from them in a columnar database for future analysis
2. Catch any events we define as useful and propagate them in real time using an in-house streaming API

Flow of data within the application<br>
<img src="https://i.imgur.com/LQ6mh56.png" style="width:50%" /><br>

Formatting of the JSON messages within RabbitMQ and the internal stream broadcast:

<ul>
    ALL messages have the following keys
    <li>stream: The origin stream (company, filing, etc)</li>
    <li>event_type: Description of the event. More info below</li>
    <li>event_published_at: Event published field in format "%Y-%m-%d %H:%M:%S" GMT+0</li>
    <li>event_id: The UUID4 of the event in the database</li>
    <li>company_number: The company that the event corresponds to </li>
</ul>

<ul>
    Company stream
    <li>event_type: company.update or company.new</li>
    <li>fields_changed: Comma seperated list of all changed fields for the company.<br>
        Follows format of the database table, with underscores replacing dots <br>
        Present when type = company.update.</li>
</ul>