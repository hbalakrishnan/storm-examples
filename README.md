# storm-tutorial using pubnub

Its a simple example which simulates sales event and publish to pubnub channel. Then a simple storm topology which has a pubnub spout which consumes messages from the channel which aggregates the sales event.

## Steps to Run
1. clone this repo
2. Create an app in https://admin.pubnub.com/, update the subscriber and publisher keys in config.json
3. mvn clean install
4. Run globalsales simulator 

