/*
 * ESP32-C3 Featured FreeRTOS IoT Integration V202204.00
 * Copyright (C) 2022 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * https://www.FreeRTOS.org
 * https://github.com/FreeRTOS
 *
 */

/*
 * This file demonstrates numerous tasks all of which use the core-MQTT Agent API
 * to send unique MQTT payloads to unique topics over the same MQTT connection
 * to the same coreMQTT-Agent.
 *
 * Each created task is a unique instance of the task implemented by
 * prvQuickConnectV2Task().  prvQuickConnectV2Task()
 * subscribes to a topic, publishes a message to the same
 * topic, receives the message, then unsubscribes from the topic in a loop.
 * The command context sent to MQTTAgent_Publish() contains a unique number that is sent back to the task
 * as a task notification from the callback function that executes when the
 * operations are acknowledged (or just sent in the case of QoS 0).  The
 * task checks the number it receives from the callback equals the number it
 * previously set in the command context before printing out either a success
 * or failure message.
 */

/* Includes *******************************************************************/

/* Standard includes. */
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* FreeRTOS includes. */
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

/* ESP-IDF includes. */
#include "esp_log.h"
#include "esp_event.h"
#include "sdkconfig.h"

/* coreMQTT library include. */
#include "core_mqtt.h"

/* coreMQTT-Agent include. */
#include "core_mqtt_agent.h"

/* coreMQTT-Agent network manager include. */
#include "core_mqtt_agent_manager.h"
#include "core_mqtt_agent_manager_events.h"

/* Subscription manager include. */
#include "subscription_manager.h"

/* Public functions include. */
#include "quickconnect_v2_demo.h"

/* Demo task configurations include. */
#include "quickconnect_v2_demo_config.h"

/* Hardware drivers include. */
#include "app_driver.h"

/* coreMQTT-Agent manager configurations include. */
#include "core_mqtt_agent_manager_config.h"

/* Preprocessor definitions ***************************************************/

/* coreMQTT-Agent event group bit definitions */
#define CORE_MQTT_AGENT_CONNECTED_BIT              ( 1 << 0 )
#define CORE_MQTT_AGENT_OTA_NOT_IN_PROGRESS_BIT    ( 1 << 1 )

/* MQTT event group bit definitions. */
#define MQTT_PUBLISH_COMMAND_COMPLETED_BIT         ( 1 << 1 )

/* Struct definitions *********************************************************/

/**
 * @brief Defines the structure to use as the command callback context in this
 * demo.
 */
struct MQTTAgentCommandContext
{
    MQTTStatus_t xReturnStatus;
    EventGroupHandle_t xMqttEventGroup;
    void * pArgs;
};

/**
 * @brief Parameters for this task.
 */
struct DemoParams
{
    uint32_t ulTaskNumber;
};

/* Global variables ***********************************************************/

/**
 * @brief Logging tag for ESP-IDF logging functions.
 */
static const char * TAG = "quickconnect_v2_demo";

/**
 * @brief Static handle used for MQTT agent context.
 */
extern MQTTAgentContext_t xGlobalMqttAgentContext;

/**
 * @brief The buffer to hold the topic filter. The topic is generated at runtime
 * by adding the task names.
 *
 */
static char topicBuf[ quickconnectv2configNUM_TASKS_TO_CREATE ][ quickconnectv2configSTRING_BUFFER_LENGTH ];

/**
 * @brief The event group used to manage coreMQTT-Agent events.
 */
static EventGroupHandle_t xNetworkEventGroup;

/**
 * @brief The semaphore used to lock access to ulMessageID to eliminate a race
 * condition in which multiple tasks try to increment/get ulMessageID.
 */
static SemaphoreHandle_t xMessageIdSemaphore;

/**
 * @brief The message ID for the next message sent by this demo.
 */
static uint32_t ulMessageId = 0;

/* Static function declarations ***********************************************/

/**
 * @brief ESP Event Loop library handler for coreMQTT-Agent events.
 *
 * This handles events defined in core_mqtt_agent_events.h.
 */
static void prvCoreMqttAgentEventHandler( void * pvHandlerArg,
                                          esp_event_base_t xEventBase,
                                          int32_t lEventId,
                                          void * pvEventData );

/**
 * @brief Passed into MQTTAgent_Publish() as the callback to execute when the
 * broker ACKs the PUBLISH message.  Its implementation sends a notification
 * to the task that called MQTTAgent_Publish() to let the task know the
 * PUBLISH operation completed.  It also sets the xReturnStatus of the
 * structure passed in as the command's context to the value of the
 * xReturnStatus parameter - which enables the task to check the status of the
 * operation.
 *
 * See https://freertos.org/mqtt/mqtt-agent-demo.html#example_mqtt_api_call
 *
 * @param[in] pxCommandContext Context of the initial command.
 * @param[in].xReturnStatus The result of the command.
 */
static void prvPublishCommandCallback( MQTTAgentCommandContext_t * pxCommandContext,
                                       MQTTAgentReturnInfo_t * pxReturnInfo );

/**
 * @brief Called by the task to wait for event from a callback function
 * after the task first executes either MQTTAgent_Publish()
 *
 * See https://freertos.org/mqtt/mqtt-agent-demo.html#example_mqtt_api_call
 *
 * @param[in] xMqttEventGroup Event group used for MQTT events.
 * @param[in] uxBitsToWaitFor Event to wait for.
 *
 * @return Received event.
 */
static EventBits_t prvWaitForEvent( EventGroupHandle_t xMqttEventGroup,
                                    EventBits_t uxBitsToWaitFor );
                                    
/**
 * @brief The function that implements the task demonstrated by this file.
 */
static void prvQuickConnectV2Task( void * pvParameters );

/* Static function definitions ************************************************/

static void prvCoreMqttAgentEventHandler( void * pvHandlerArg,
                                          esp_event_base_t xEventBase,
                                          int32_t lEventId,
                                          void * pvEventData )
{
    ( void ) pvHandlerArg;
    ( void ) xEventBase;
    ( void ) pvEventData;

    switch( lEventId )
    {
        case CORE_MQTT_AGENT_CONNECTED_EVENT:
            ESP_LOGI( TAG,
                      "coreMQTT-Agent connected." );
            xEventGroupSetBits( xNetworkEventGroup,
                                CORE_MQTT_AGENT_CONNECTED_BIT );
            break;

        case CORE_MQTT_AGENT_DISCONNECTED_EVENT:
            ESP_LOGI( TAG,
                      "coreMQTT-Agent disconnected. Preventing coreMQTT-Agent "
                      "commands from being enqueued." );
            xEventGroupClearBits( xNetworkEventGroup,
                                  CORE_MQTT_AGENT_CONNECTED_BIT );
            break;

        default:
            ESP_LOGE( TAG,
                      "coreMQTT-Agent event handler received unexpected event: %" PRIu32 "",
                      lEventId );
            break;
    }
}

static void prvPublishCommandCallback( MQTTAgentCommandContext_t * pxCommandContext,
                                       MQTTAgentReturnInfo_t * pxReturnInfo )
{
    /* Store the result in the application defined context so the task that
     * initiated the publish can check the operation's status. */
    pxCommandContext->xReturnStatus = pxReturnInfo->returnCode;

    if( pxCommandContext->xMqttEventGroup != NULL )
    {
        xEventGroupSetBits( pxCommandContext->xMqttEventGroup,
                            MQTT_PUBLISH_COMMAND_COMPLETED_BIT );
    }
}

static EventBits_t prvWaitForEvent( EventGroupHandle_t xMqttEventGroup,
                                    EventBits_t uxBitsToWaitFor )
{
    EventBits_t xReturn;

    xReturn = xEventGroupWaitBits( xMqttEventGroup,
                                   uxBitsToWaitFor,
                                   pdTRUE, /* xClearOnExit. */
                                   pdTRUE, /* xWaitForAllBits. */
                                   portMAX_DELAY );
    return xReturn;
}

static void prvPublishToTopic( MQTTQoS_t xQoS,
                               char * pcTopicName,
                               char * pcPayload,
                               EventGroupHandle_t xMqttEventGroup )
{
    uint32_t ulPublishMessageId = 0;

    MQTTStatus_t xCommandAdded;
    EventBits_t xReceivedEvent = 0;

    MQTTPublishInfo_t xPublishInfo = { 0 };

    MQTTAgentCommandContext_t xCommandContext = { 0 };
    MQTTAgentCommandInfo_t xCommandParams = { 0 };

    xTaskNotifyStateClear( NULL );

    /* Create a unique number for the publish that is about to be sent.
     * This number is used in the command context and is sent back to this task
     * as a notification in the callback that's executed upon receipt of the
     * publish from coreMQTT-Agent.
     * That way this task can match an acknowledgment to the message being sent.
     */
    xSemaphoreTake( xMessageIdSemaphore, portMAX_DELAY );
    {
        ++ulMessageId;
        ulPublishMessageId = ulMessageId;
    }
    xSemaphoreGive( xMessageIdSemaphore );

    /* Configure the publish operation. The topic name string must persist for
     * duration of publish! */
    xPublishInfo.qos = xQoS;
    xPublishInfo.pTopicName = pcTopicName;
    xPublishInfo.topicNameLength = ( uint16_t ) strlen( pcTopicName );
    xPublishInfo.pPayload = pcPayload;
    xPublishInfo.payloadLength = ( uint16_t ) strlen( pcPayload );

    /* Complete an application defined context associated with this publish
     * message.
     * This gets updated in the callback function so the variable must persist
     * until the callback executes. */
    xCommandContext.xMqttEventGroup = xMqttEventGroup;

    xCommandParams.blockTimeMs = quickconnectv2configMAX_COMMAND_SEND_BLOCK_TIME_MS;
    xCommandParams.cmdCompleteCallback = prvPublishCommandCallback;
    xCommandParams.pCmdCompleteCallbackContext = &xCommandContext;

    do
    {
        /* Wait for coreMQTT-Agent task to have working network connection */
        xEventGroupWaitBits( xNetworkEventGroup,
                             CORE_MQTT_AGENT_CONNECTED_BIT | CORE_MQTT_AGENT_OTA_NOT_IN_PROGRESS_BIT,
                             pdFALSE,
                             pdTRUE,
                             portMAX_DELAY );

        ESP_LOGI( TAG,
                  "Task \"%s\" sending publish request to coreMQTT-Agent with message \"%s\" on topic \"%s\" with ID %" PRIu32 ".",
                  pcTaskGetName( NULL ),
                  pcPayload,
                  pcTopicName,
                  ulPublishMessageId );

        xCommandAdded = MQTTAgent_Publish( &xGlobalMqttAgentContext,
                                           &xPublishInfo,
                                           &xCommandParams );

        if( xCommandAdded == MQTTSuccess )
        {
            /* For QoS 1 and 2, wait for the publish acknowledgment.  For QoS0,
             * wait for the publish to be sent. */
            ESP_LOGI( TAG,
                      "Task \"%s\" waiting for publish %" PRIu32 " to complete.",
                      pcTaskGetName( NULL ),
                      ulPublishMessageId );

            xReceivedEvent = prvWaitForEvent( xMqttEventGroup,
                                              MQTT_PUBLISH_COMMAND_COMPLETED_BIT );
        }
        else
        {
            ESP_LOGE( TAG,
                      "Failed to enqueue publish command. Error code=%s",
                      MQTT_Status_strerror( xCommandAdded ) );
        }

        /* Check all ways the status was passed back just for demonstration
         * purposes. */
        if( ( ( xReceivedEvent & MQTT_PUBLISH_COMMAND_COMPLETED_BIT ) == 0 ) ||
            ( xCommandContext.xReturnStatus != MQTTSuccess ) )
        {
            ESP_LOGW( TAG,
                      "Error or timed out waiting for ack for publish message %" PRIu32 ". Re-attempting publish.",
                      ulPublishMessageId );
        }
        else
        {
            ESP_LOGI( TAG,
                      "Publish %" PRIu32 " succeeded for task \"%s\".",
                      ulPublishMessageId,
                      pcTaskGetName( NULL ) );
        }
    } while( ( xReceivedEvent & MQTT_PUBLISH_COMMAND_COMPLETED_BIT ) == 0 ||
             ( xCommandContext.xReturnStatus != MQTTSuccess ) );
}

static void prvQuickConnectV2Task( void * pvParameters )
{
    struct DemoParams * pxParams = ( struct DemoParams * ) pvParameters;
    uint32_t ulTaskNumber = pxParams->ulTaskNumber;

    EventGroupHandle_t xMqttEventGroup;

    MQTTQoS_t xQoS;
    char * pcTopicBuffer = topicBuf[ ulTaskNumber ];
    char pcPayload[ quickconnectv2configSTRING_BUFFER_LENGTH ];
    float temperatureValue;

    xMqttEventGroup = xEventGroupCreate();

    xQoS = ( MQTTQoS_t ) quickconnectv2configQOS_LEVEL;

    /* Take the topic name from thing name. */
    snprintf( pcTopicBuffer,
              quickconnectv2configSTRING_BUFFER_LENGTH,
              "%s",
              configCLIENT_IDENTIFIER );

    while( 1 )
    {
        temperatureValue = app_driver_temp_sensor_read_celsius();
        
        /* Create Payload in an Array format */
        snprintf( pcPayload,
                  quickconnectv2configSTRING_BUFFER_LENGTH,
                  "[{\"label\":\"ESP32-S3 MCU Temperature\",\"display_type\":\"line_graph\",\"unit\":\"C\",\"values\":[{\"value\":%.1f,\"label\":\"temp\"}]}]",
                  temperatureValue );

        prvPublishToTopic( xQoS,
                           pcTopicBuffer,
                           pcPayload,
                           xMqttEventGroup );

        ESP_LOGI( TAG,
                  "Task \"%s\" published: %s",
                  pcTaskGetName( NULL ),
                  pcPayload );

        ESP_LOGI( TAG,
                  "Task \"%s\" completed a loop. Delaying before next loop.",
                  pcTaskGetName( NULL ) );

        vTaskDelay( pdMS_TO_TICKS( quickconnectv2configDELAY_BETWEEN_LOOPS_MS ) );
    }

    vEventGroupDelete( xMqttEventGroup );
    vTaskDelete( NULL );
}

/* Public function definitions ************************************************/

void vStartQuickConnectV2Demo( void )
{
    static struct DemoParams pxParams[ quickconnectv2configNUM_TASKS_TO_CREATE ];
    char pcTaskNameBuf[ 30 ];
    uint32_t ulTaskNumber;

    xMessageIdSemaphore = xSemaphoreCreateMutex();
    xNetworkEventGroup = xEventGroupCreate();
    xCoreMqttAgentManagerRegisterHandler( prvCoreMqttAgentEventHandler );

    /* Initialize the coreMQTT-Agent event group. */
    xEventGroupSetBits( xNetworkEventGroup,
                        CORE_MQTT_AGENT_OTA_NOT_IN_PROGRESS_BIT );

    /* Each instance of prvQuickConnectV2Task() generates a unique
     * name and topic filter for itself from the number passed in as the task
     * parameter. */
    /* Create a few instances of prvQuickConnectV2Task(). */
    for( ulTaskNumber = 0; ulTaskNumber < quickconnectv2configNUM_TASKS_TO_CREATE; ulTaskNumber++ )
    {
        memset( pcTaskNameBuf,
                0x00,
                sizeof( pcTaskNameBuf ) );

        snprintf( pcTaskNameBuf,
                  30,
                  "DemoTask");

        pxParams[ ulTaskNumber ].ulTaskNumber = ulTaskNumber;

        xTaskCreate( prvQuickConnectV2Task,
                     pcTaskNameBuf,
                     quickconnectv2configTASK_STACK_SIZE,
                     ( void * ) &pxParams[ ulTaskNumber ],
                     quickconnectv2configTASK_PRIORITY,
                     NULL );
    }
}
