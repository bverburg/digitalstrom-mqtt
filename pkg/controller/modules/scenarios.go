package modules

import (
	"fmt"
	"path"
	"strings"
	"time"

	mqtt_base "github.com/eclipse/paho.mqtt.golang"
	"github.com/gaetancollaud/digitalstrom-mqtt/pkg/config"
	"github.com/gaetancollaud/digitalstrom-mqtt/pkg/digitalstrom"
	"github.com/gaetancollaud/digitalstrom-mqtt/pkg/homeassistant"
	"github.com/gaetancollaud/digitalstrom-mqtt/pkg/mqtt"
	"github.com/rs/zerolog/log"
)

// Scenario Module encapsulates all the logic regarding scenarios. The logic
// is the following: scenarios states can be changed from mqtt and forwarded to digitalstrom on the opposite
// side, when an event is received from digitalstrom, the new value is pushed to mqtt.
type ScenarioModule struct {
	mqttClient mqtt.Client
	dsClient   digitalstrom.Client
	dsRegistry digitalstrom.Registry

	refreshAtStart bool
}

func (c *ScenarioModule) Start() error {
	scenarios, err := c.dsRegistry.GetScenarios()

	for _, scenario := range scenarios {
		err := c.dsRegistry.ScenarioChangeSubscribe(scenario.ScenarioId, func(scenarioId string, oldValue digitalstrom.ScenarioState, newValue digitalstrom.ScenarioState, scheduledAt time.Time, terminatedAt time.Time) {
			err := c.updateScenario(scenarioId)
			if err != nil {
				log.Error().Err(err).Str("scenarioid", scenarioId).Msg("Error updating scenario ")
			}
		})
		if err != nil {
			return err
		}
	}

	if err == nil {
		// Refresh scenario values.
		if c.refreshAtStart {
			go func() {
				for _, scenario := range scenarios {
					if err := c.updateScenario(scenario.ScenarioId); err != nil {
						log.Error().Err(err).Msgf("Error updating scenario '%s'", scenario.Attributes.Name)
					}
				}
			}()
		}
	}

	// Subscribe to MQTT events.
	for _, scenario := range scenarios {

		scenarioId := scenario.ScenarioId        // deep copy
		scenarioName := scenario.Attributes.Name // deep copy

		topic := c.scenarioCommandTopic(scenarioName)
		log.Trace().
			Str("topic", topic).
			Str("scenarioName", scenarioName).
			Msg("Subscribing for topic.")
		err := c.mqttClient.Subscribe(topic, func(client mqtt_base.Client, message mqtt_base.Message) {
			payload := string(message.Payload())
			log.Trace().
				Str("topic", topic).
				Str("scenarioName", scenarioName).
				Str("payload", payload).
				Msg("Message Received.")
			if err := c.onMqttMessage(scenarioId, payload); err != nil {
				log.Error().
					Str("topic", topic).
					Err(err).
					Msg("Error handling MQTT Message.")
			}
		})
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *ScenarioModule) Stop() error {
	if devices, err := c.dsRegistry.GetDevices(); err != nil {
		for _, device := range devices {
			_ = c.dsRegistry.DeviceChangeUnsubscribe(device.DeviceId)
		}
	}

	return nil
}

func (c *ScenarioModule) onMqttMessage(scenarioId string, message string) error {
	scenario, err := c.dsRegistry.GetScenario(scenarioId)
	if err != nil {
		return err
	}

	//either active or inactive
	value, err := digitalstrom.StringToScenarioState(strings.TrimSpace(message)) // strconv.ParseFloat(strings.TrimSpace(message), 64)
	if err != nil {
		return fmt.Errorf("error parsing message as ScenarioState value: %w", err)
	}

	log.Info().
		Str("device", scenario.Attributes.Name).
		Str("value", string(value)).
		Msg("Setting value.")

	if value == digitalstrom.ScenarioStateActive {

		err = c.dsClient.ScenarioInvoke(scenarioId)
		if err != nil {
			return err
		}
	}

	// for fast deliveries we confirm the state
	if err := c.publishScenarioValue(&scenario, value); err != nil {
		return err
	}

	return nil
}

func (c *ScenarioModule) updateScenario(scenarioId string) error {
	scenario, err := c.dsRegistry.GetScenario(scenarioId)
	if err != nil {
		return err
	}
	value, err := c.dsRegistry.GetValueOfScenario(scenarioId)

	log.Debug().
		Str("scenario", scenario.Attributes.Name).
		Msg("Updating Scenario")

	if err := c.publishScenarioValue(&scenario, value); err != nil {
		return fmt.Errorf("error publishing device '%s' value: %w", scenario.Attributes.Name, err)
	}

	return nil
}

func (c *ScenarioModule) publishScenarioValue(scenario *digitalstrom.Scenario, value digitalstrom.ScenarioState) error {
	return c.mqttClient.Publish(c.scenarioStateTopic(scenario.Attributes.Name), fmt.Sprintf("%s", value))
}

func (c *ScenarioModule) scenarioStateTopic(scenarioName string) string {

	return path.Join(devices, scenarioName, mqtt.State)
}

func (c *ScenarioModule) scenarioCommandTopic(scenarioName string) string {

	return path.Join(devices, scenarioName, mqtt.Command)
}

func (c *ScenarioModule) GetHomeAssistantEntities() ([]homeassistant.DiscoveryConfig, error) {
	configs := []homeassistant.DiscoveryConfig{}

	return configs, nil
}

func NewScenarioModule(mqttClient mqtt.Client, dsClient digitalstrom.Client, dsRegistry digitalstrom.Registry, config *config.Config) Module {
	return &ScenarioModule{
		mqttClient:     mqttClient,
		dsClient:       dsClient,
		dsRegistry:     dsRegistry,
		refreshAtStart: config.RefreshAtStart,
	}
}

func init() {
	Register("scenarios", NewScenarioModule)
}
