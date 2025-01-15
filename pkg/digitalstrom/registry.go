package digitalstrom

import (
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type ScenarioChangeCallback func(scenarioId string, oldValue ScenarioState, newValue ScenarioState, scheduledAt time.Time, terminatesAt time.Time)

type DeviceChangeCallback func(deviceId string, outputId string, oldValue float64, newValue float64)

// Registry The registry hold the current structure of the appartement and the latest known state
type Registry interface {
	Start() error

	Stop() error

	GetDevices() ([]Device, error)
	GetScenarios() ([]Scenario, error)

	GetDevice(deviceId string) (Device, error)
	GetScenario(scenarioId string) (Scenario, error)

	GetFunctionBlockForDevice(deviceId string) (FunctionBlock, error)

	GetOutputsOfDevice(deviceId string) ([]Output, error)
	GetOutputValuesOfDevice(deviceId string) ([]OutputValue, error)
	GetValueOfScenario(scenarioId string) (ScenarioState, error)

	GetControllers() ([]Controller, error)
	GetControllerById(controllerId string) (Controller, error)
	GetMeterings() ([]Metering, error)

	DeviceChangeSubscribe(deviceId string, callback DeviceChangeCallback) error
	DeviceChangeUnsubscribe(deviceId string) error

	ScenarioChangeSubscribe(scenarioId string, callback ScenarioChangeCallback) error
	ScenarioChangeUnsubscribe(scenarioId string) error
}

type registry struct {
	digitalstromClient Client

	apartment       *Apartment
	apartmentStatus *ApartmentStatus
	meterings       *Meterings

	controllersLookup    map[string]Controller
	devicesLookup        map[string]Device
	scenariosLookup      map[string]Scenario
	submoduleLookup      map[string]Submodule
	functionBlocksLookup map[string]FunctionBlock

	deviceChangeCallbacks   map[string]DeviceChangeCallback
	scenarioChangeCallbacks map[string]ScenarioChangeCallback

	registryLoading sync.Mutex
}

func NewRegistry(digitalstromClient Client) Registry {
	return &registry{
		digitalstromClient:      digitalstromClient,
		deviceChangeCallbacks:   make(map[string]DeviceChangeCallback),
		scenarioChangeCallbacks: make(map[string]ScenarioChangeCallback),
	}
}

func (r *registry) Start() error {
	if err := r.updateApartment(); err != nil {
		return err
	}
	if err := r.updateMeterings(); err != nil {
		return err
	}
	err := r.updateApartmentStatusAndFireChangeEvents()
	if err != nil {
		return err
	}
	callback := func(notification WebsocketNotification) {
		// TODO handle structure changes
		if err := r.updateApartmentStatusAndFireChangeEvents(); err != nil {
			log.Err(err).Msg("Error updating apartment status")
		}
	}
	if err := r.digitalstromClient.NotificationSubscribe("registry", callback); err != nil {
		return err
	}
	return nil
}

func (r *registry) Stop() error {
	return nil
}

func (r *registry) GetScenarios() ([]Scenario, error) {
	return r.apartment.Included.Scenarios, nil
}

func (r *registry) GetScenario(scenarioId string) (Scenario, error) {
	scenario, ok := r.scenariosLookup[scenarioId]
	if ok {
		return scenario, nil
	}
	return Scenario{}, errors.New("No scenario found with id " + scenarioId)
}

func (r *registry) GetDevices() ([]Device, error) {
	return r.apartment.Included.Devices, nil
}

func (r *registry) GetDevice(deviceId string) (Device, error) {
	device, ok := r.devicesLookup[deviceId]
	if ok {
		return device, nil
	}
	return Device{}, errors.New("No device found with id " + deviceId)
}

func (r *registry) GetOutputsOfDevice(deviceId string) ([]Output, error) {
	device, err := r.GetDevice(deviceId)
	if err != nil {
		return nil, err
	}

	outputs := []Output{}
	for _, submoduleId := range device.Attributes.Submodules {
		submodule := r.submoduleLookup[submoduleId]
		for _, functionBlockId := range submodule.Attributes.FunctionBlocks {
			functionBlock := r.functionBlocksLookup[functionBlockId]
			for _, output := range functionBlock.Attributes.Outputs {
				outputs = append(outputs, output)
			}
		}
	}

	return outputs, nil
}

func (r *registry) GetOutputValuesOfDevice(deviceId string) ([]OutputValue, error) {
	outputs := []OutputValue{}
	for _, device := range r.apartmentStatus.Included.Devices {
		if device.DeviceId == deviceId {
			for _, functionBlockValue := range device.Attributes.FunctionBlocks {
				for _, outputValue := range functionBlockValue.Outputs {
					outputs = append(outputs, outputValue)
				}
			}
		}
	}

	return outputs, nil
}

// GetValueOfScenario implements Registry.
func (r *registry) GetValueOfScenario(scenarioId string) (ScenarioState, error) {

	for _, zone := range r.apartmentStatus.Included.Zones {
		for _, scenario := range zone.Attributes.Scenarios {
			if scenario.ScenarioId == scenarioId {
				return scenario.Status, nil
			}
		}
	}

	return ScenarioStateUnknown, nil
}

func (r *registry) GetFunctionBlockForDevice(deviceId string) (FunctionBlock, error) {
	device, err := r.GetDevice(deviceId)
	if err != nil {
		return FunctionBlock{}, err
	}

	var functionBlocks []FunctionBlock

	for _, submoduleId := range device.Attributes.Submodules {
		submodule := r.submoduleLookup[submoduleId]
		for _, functionBlockId := range submodule.Attributes.FunctionBlocks {
			functionBlock := r.functionBlocksLookup[functionBlockId]
			functionBlocks = append(functionBlocks, functionBlock)
		}
	}

	length := len(functionBlocks)
	if length == 0 {
		return FunctionBlock{}, errors.New("Multiple function blocks found for device " + deviceId)
	}
	if length > 1 {
		return FunctionBlock{}, errors.New("No function block found for device " + deviceId)
	}
	return functionBlocks[0], nil
}

func (r *registry) GetControllers() ([]Controller, error) {
	return r.apartment.Included.Controllers, nil
}

func (r *registry) GetControllerById(controllerId string) (Controller, error) {
	controller, ok := r.controllersLookup[controllerId]
	if ok {
		return controller, nil
	}
	return Controller{}, errors.New("No controller found with id " + controllerId)
}

func (r *registry) GetMeterings() ([]Metering, error) {
	return r.meterings.Meterings, nil
}

func (r *registry) updateApartment() error {
	r.registryLoading.Lock()
	defer r.registryLoading.Unlock()

	apartment, err := r.digitalstromClient.GetApartment()
	if err != nil {
		return err
	}

	r.apartment = apartment

	r.controllersLookup = make(map[string]Controller)
	r.devicesLookup = make(map[string]Device)
	r.scenariosLookup = make(map[string]Scenario)
	r.submoduleLookup = make(map[string]Submodule)
	r.functionBlocksLookup = make(map[string]FunctionBlock)

	// Create lookup tables for fast access.
	for _, controller := range apartment.Included.Controllers {
		r.controllersLookup[controller.ControllerId] = controller
	}
	for _, device := range apartment.Included.Devices {
		r.devicesLookup[device.DeviceId] = device
	}
	for _, submodule := range apartment.Included.Submodules {
		r.submoduleLookup[submodule.SubmoduleId] = submodule
	}
	for _, functionBlock := range apartment.Included.FunctionBlocks {
		r.functionBlocksLookup[functionBlock.FunctionBlockId] = functionBlock
	}
	for _, scenario := range apartment.Included.Scenarios {
		r.scenariosLookup[scenario.ScenarioId] = scenario
	}

	return nil
}

func (r *registry) updateMeterings() error {
	r.registryLoading.Lock()
	defer r.registryLoading.Unlock()

	meterings, err := r.digitalstromClient.GetMeterings()
	if err != nil {
		return err
	}

	r.meterings = meterings

	return nil
}

func (r *registry) DeviceChangeSubscribe(deviceId string, callback DeviceChangeCallback) error {
	_, exists := r.deviceChangeCallbacks[deviceId]
	if exists {
		return errors.New("Callback already registered for device " + deviceId)
	}
	r.deviceChangeCallbacks[deviceId] = callback
	return nil
}

func (r *registry) DeviceChangeUnsubscribe(deviceId string) error {
	_, exists := r.deviceChangeCallbacks[deviceId]
	if !exists {
		return errors.New("No callback registered for device " + deviceId)
	}
	delete(r.deviceChangeCallbacks, deviceId)
	return nil
}

func (r *registry) ScenarioChangeSubscribe(scenarioId string, callback ScenarioChangeCallback) error {
	_, exists := r.scenarioChangeCallbacks[scenarioId]
	if exists {
		return errors.New("Callback already registered for scenario " + scenarioId)
	}
	r.scenarioChangeCallbacks[scenarioId] = callback
	return nil
}

func (r *registry) ScenarioChangeUnsubscribe(scenarioId string) error {
	_, exists := r.scenarioChangeCallbacks[scenarioId]
	if !exists {
		return errors.New("No callback registered for scenario " + scenarioId)
	}
	delete(r.scenarioChangeCallbacks, scenarioId)
	return nil
}

func (r *registry) updateApartmentStatusAndFireChangeEvents() error {
	oldStatus := r.apartmentStatus
	newStatus, err := r.digitalstromClient.GetApartmentStatus()
	if err != nil {
		return err
	}
	r.apartmentStatus = newStatus

	if oldStatus != nil {
		// Check diff and broadcast events
		oldStatusDeviceLookup := make(map[string]map[string]OutputValue)
		for _, device := range oldStatus.Included.Devices {
			oldStatusDeviceLookup[device.DeviceId] = make(map[string]OutputValue)
			for _, functionBlock := range device.Attributes.FunctionBlocks {
				for _, output := range functionBlock.Outputs {
					oldStatusDeviceLookup[device.DeviceId][output.OutputId] = output
				}
			}
		}
		oldStatusScenarioLookup := make(map[string]ScenarioState)
		for _, zone := range oldStatus.Included.Zones {
			for _, scenario := range zone.Attributes.Scenarios {
				oldStatusScenarioLookup[scenario.ScenarioId] = scenario.Status
			}
		}

		for _, zone := range newStatus.Included.Zones {
			for _, scenario := range zone.Attributes.Scenarios {
				oldStatus := oldStatusScenarioLookup[scenario.ScenarioId]
				if oldStatus != scenario.Status {
					log.Info().
						Str("ScenarioId", scenario.ScenarioId).
						Str("oldValue", string(oldStatus)).
						Str("newValue", string(scenario.Status)).
						Msg("Scene invoked")

					callback, exists := r.scenarioChangeCallbacks[scenario.ScenarioId]
					if exists {
						t := time.Now()
						callback(scenario.ScenarioId, oldStatus, scenario.Status, t, t)
					}
				}
			}
		}

		for _, device := range newStatus.Included.Devices {
			for _, functionBlock := range device.Attributes.FunctionBlocks {
				for _, newOutput := range functionBlock.Outputs {
					oldOutput := oldStatusDeviceLookup[device.DeviceId][newOutput.OutputId]
					if oldOutput.TargetValue != newOutput.TargetValue {
						log.Info().
							Str("DeviceId", device.DeviceId).
							Str("Output", newOutput.OutputId).
							Float64("oldValue", oldOutput.TargetValue).
							Float64("newValue", newOutput.TargetValue).
							Msg("Output value changed")

						callback, exists := r.deviceChangeCallbacks[device.DeviceId]
						if exists {
							callback(device.DeviceId, newOutput.OutputId, oldOutput.TargetValue, newOutput.TargetValue)
						}
					}
				}
			}
		}
	}
	return nil
}
