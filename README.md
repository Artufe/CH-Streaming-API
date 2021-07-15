# Companies House Streaming Data
Companies House offers a relatively new service called "Streaming API"<br>
This feature offers a real-time feed of data pertaining to updates of data within Companies House<br>

<b>The aim of this project is:</b> <br>
1. Gather all the streams and store the data from them in a columnar database for future analysis
2. Catch any events we define as useful and propagate them in real time using an in-house streaming API

Flow of data within the application<br>
<img src="https://i.imgur.com/LQ6mh56.png" style="width:50%" /><br>

Foratting of the JSON messages within RabbitMQ and the internal stream broadcast:
<br><br>
