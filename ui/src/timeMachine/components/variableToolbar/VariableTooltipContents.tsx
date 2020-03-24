// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

// Components
import {Form} from '@influxdata/clockface'
import VariableDropdown from 'src/variables/components/VariableDropdown'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  timeMachineID: string
}

interface DispatchProps {
  execute: typeof executeQueries
}

interface OwnProps {
  variableID: string
}

type Props = StateProps & DispatchProps & OwnProps

const VariableTooltipContents: FunctionComponent<Props> = ({
  variableID,
  timeMachineID,
  execute,
}) => {
  const refresh = () => {
    execute()
  }
  return (
    <div>
      <Form.Element label="Value">
        <VariableDropdown
          variableID={variableID}
          contextID={timeMachineID}
          onSelect={refresh}
          testID="variable--tooltip-dropdown"
        />
      </Form.Element>
    </div>
  )
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const contextID =
    timeMachine.contextID || state.timeMachines.activeTimeMachineID

  return {
    timeMachineID: contextID,
  }
}

const mdtp = {
  execute: executeQueries,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableTooltipContents)
