package samples

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
)

func CreateClient(connectionString string) {
	// region createClient
	conf, err := kurrentdb.ParseConnectionString(connectionString)

	if err != nil {
		panic(err)
	}

	client, err := kurrentdb.NewProjectionClient(conf)

	if err != nil {
		panic(err)
	}
	// endregion createClient

	defer client.Close()
}

func Disable(client *kurrentdb.ProjectionClient) {
	// region disable
	err := client.Disable(context.Background(), "$by_category", kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion disable
}

func DisableNotFound(client *kurrentdb.ProjectionClient) {
	// region disableNotFound
	err := client.Disable(context.Background(), "projection that doesn't exist", kurrentdb.GenericProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion disableNotFound
}

func Enable(client *kurrentdb.ProjectionClient) {
	// region enable
	err := client.Enable(context.Background(), "$by_category", kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion enable
}

func EnableNotFound(client *kurrentdb.ProjectionClient) {
	// region enableNotFound
	err := client.Enable(context.Background(), "projection that doesn't exist", kurrentdb.GenericProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion enableNotFound
}

func Delete(client *kurrentdb.ProjectionClient) {
	// region delete
	err := client.Delete(context.Background(), "$by_category", kurrentdb.DeleteProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion delete
}

func DeleteNotFound(client *kurrentdb.ProjectionClient) {
	// region deleteNotFound
	err := client.Delete(context.Background(), "projection that doesn't exist", kurrentdb.DeleteProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion deleteNotFound
}

func Abort(client *kurrentdb.ProjectionClient) {
	// region abort
	err := client.Abort(context.Background(), "$by_category", kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion abort
}

func AbortNotFound(client *kurrentdb.ProjectionClient) {
	// region abortNotFound
	err := client.Abort(context.Background(), "projection that doesn't exist", kurrentdb.GenericProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion abortNotFound
}

func Reset(client *kurrentdb.ProjectionClient) {
	// region reset
	err := client.Reset(context.Background(), "$by_category", kurrentdb.ResetProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion reset
}

func ResetNotFound(client *kurrentdb.ProjectionClient) {
	// region resetNotFound
	err := client.Reset(context.Background(), "projection that doesn't exist", kurrentdb.ResetProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion resetNotFound
}

func Create(client *kurrentdb.ProjectionClient) {
	// region createContinuous
	script := `
fromAll()
.when({
	$init:function(){
		return {
			count: 0
		}
	},
	myEventUpdatedType: function(state, event){
		state.count += 1;
	}
})
.transformBy(function(state){
	state.count = 10;
})
.outputState()
`
	name := fmt.Sprintf("countEvent_Create_%s", uuid.New())
	err := client.Create(context.Background(), name, script, kurrentdb.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	// endregion createContinuous
}

func CreateConflict(client *kurrentdb.ProjectionClient) {
	script := ""
	name := ""

	// region createContinuousConflict
	err := client.Create(context.Background(), name, script, kurrentdb.CreateProjectionOptions{})

	if esdbErr, ok := kurrentdb.FromError(err); !ok {
		if esdbErr.IsErrorCode(kurrentdb.ErrorCodeUnknown) && strings.Contains(esdbErr.Err().Error(), "Conflict") {
			log.Printf("projection %s already exists", name)
			return
		}
	}
	// endregion createContinuousConflict
}

func Update(client *kurrentdb.ProjectionClient) {
	script := ""
	newScript := ""
	name := ""

	// region update
	err := client.Create(context.Background(), name, script, kurrentdb.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	err = client.Update(context.Background(), name, newScript, kurrentdb.UpdateProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion update

}

func UpdateNotFound(client *kurrentdb.ProjectionClient) {
	script := ""

	// region updateNotFound
	err := client.Update(context.Background(), "projection that doesn't exist", script, kurrentdb.UpdateProjectionOptions{})

	if esdbError, ok := kurrentdb.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrentdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion updateNotFound
}

func ListAll(client *kurrentdb.ProjectionClient) {
	// region listAll
	projections, err := client.ListAll(context.Background(), kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	for i := range projections {
		projection := projections[i]

		log.Printf(
			"%s, %s, %s, %s, %f",
			projection.Name,
			projection.Status,
			projection.CheckpointStatus,
			projection.Mode,
			projection.Progress,
		)
	}
	// endregion listAll
}

func List(client *kurrentdb.ProjectionClient) {
	// region listContinuous
	projections, err := client.ListContinuous(context.Background(), kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	for i := range projections {
		projection := projections[i]

		log.Printf(
			"%s, %s, %s, %s, %f",
			projection.Name,
			projection.Status,
			projection.CheckpointStatus,
			projection.Mode,
			projection.Progress,
		)
	}
	// endregion listContinuous
}

func GetStatus(client *kurrentdb.ProjectionClient) {
	// region getStatus
	projection, err := client.GetStatus(context.Background(), "$by_category", kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	log.Printf(
		"%s, %s, %s, %s, %f",
		projection.Name,
		projection.Status,
		projection.CheckpointStatus,
		projection.Mode,
		projection.Progress,
	)
	// endregion getStatus
}

func GetState(client *kurrentdb.ProjectionClient) {
	projectionName := ""
	// region getState
	type Foobar struct {
		Count int64
	}

	value, err := client.GetState(context.Background(), projectionName, kurrentdb.GetStateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	jsonContent, err := value.MarshalJSON()

	if err != nil {
		panic(err)
	}

	var foobar Foobar

	if err = json.Unmarshal(jsonContent, &foobar); err != nil {
		panic(err)
	}

	log.Printf("count %d", foobar.Count)
	// endregion getState
}

func GetResult(client *kurrentdb.ProjectionClient) {
	projectionName := ""
	// region getResult
	type Baz struct {
		Result int64
	}

	value, err := client.GetResult(context.Background(), projectionName, kurrentdb.GetResultProjectionOptions{})

	if err != nil {
		panic(err)
	}

	jsonContent, err := value.MarshalJSON()

	if err != nil {
		panic(err)
	}

	var baz Baz

	if err = json.Unmarshal(jsonContent, &baz); err != nil {
		panic(err)
	}

	log.Printf("result %d", baz.Result)
	// endregion getResult
}

func RestartSubSystem(client *kurrentdb.ProjectionClient) {
	// region restartSubsystem
	err := client.RestartSubsystem(context.Background(), kurrentdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion restartSubsystem
}
