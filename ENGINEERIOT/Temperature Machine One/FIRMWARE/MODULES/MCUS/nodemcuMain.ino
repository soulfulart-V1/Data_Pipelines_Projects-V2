#include <string>
#include <ESP8266WiFi.h>
#include <MCUNodeMCUEsp12E.h>
#include <ESP8266HTTPClient.h>

#define BAUD_RATE 9600
#define DELAY_MESSAGE 5000

//Objects
WiFiClient client;

//kafka credentials
string kafka_parameters_local[4]={
  "INSERT_BOOTSTRAPSERVER_DURING_COMPILE_PHASE",
  "INSERT_USER_DURING_COMPILE_PHASE",
  "INSERT_PASS_DURING_COMPILE_PHASE",
  "INSERT_KAFKA_TOPIC_DURING_COMPILE_PHASE"
  };

//wifi credentials
const char* ssid =  "SSID";
const char* pass =  "PASS";

void setup() 
{
  
       Serial.begin(BAUD_RATE);
       WiFi.begin(ssid, pass); 
       while (WiFi.status() != WL_CONNECTED) 
          {
            delay(500);
            Serial.print(".");
          }

      Serial.println("");
      Serial.println("WiFi connected");      

}
 
void loop(){

    while (WiFi.status() != WL_CONNECTED){
        delay(500);
        Serial.print(".");
        }    

    MCUNodeMCUEsp12E mcu_instance;

    string url_request = mcu_instance.urlKafkaDataProducer(kafka_parameters_local);
  
    delay(DELAY_MESSAGE);

}