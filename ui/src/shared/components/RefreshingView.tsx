// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {getTimeRange} from 'src/dashboards/selectors'
import {getAllVariables, asAssignment} from 'src/variables/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'

// Types
import {
  TimeRange,
  TimeZone,
  AppState,
  DashboardQuery,
  VariableAssignment,
  QueryViewProperties,
} from 'src/types'

interface OwnProps {
  manualRefresh: number
  properties: QueryViewProperties
}

interface StateProps {
  timeRange: TimeRange
  ranges: TimeRange | null
  timeZone: TimeZone
  variableAssignments: VariableAssignment[]
}

interface State {
  submitToken: number
}

type Props = OwnProps & StateProps

class RefreshingView extends PureComponent<Props, State> {
  public static defaultProps = {
    inView: true,
    manualRefresh: 0,
  }

  constructor(props) {
    super(props)

    this.state = {submitToken: 0}
  }

  public componentDidMount() {
    GlobalAutoRefresher.subscribe(this.incrementSubmitToken)
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.unsubscribe(this.incrementSubmitToken)
  }

  public render() {
    const {
      ranges,
      properties,
      manualRefresh,
      timeZone,
      variableAssignments,
    } = this.props
    const {submitToken} = this.state

    return (
      <TimeSeries
        submitToken={submitToken}
        queries={this.queries}
        key={manualRefresh}
        variables={variableAssignments}
      >
        {({
          giraffeResult,
          files,
          loading,
          errorMessage,
          isInitialFetch,
          statuses,
        }) => {
          return (
            <EmptyQueryView
              errorFormat={ErrorFormat.Tooltip}
              errorMessage={errorMessage}
              hasResults={checkResultsLength(giraffeResult)}
              loading={loading}
              isInitialFetch={isInitialFetch}
              queries={this.queries}
              fallbackNote={this.fallbackNote}
            >
              <ViewSwitcher
                files={files}
                giraffeResult={giraffeResult}
                loading={loading}
                properties={properties}
                timeRange={ranges}
                statuses={statuses}
                timeZone={timeZone}
              />
            </EmptyQueryView>
          )
        }}
      </TimeSeries>
    )
  }

  private get queries(): DashboardQuery[] {
    const {properties} = this.props

    switch (properties.type) {
      case 'single-stat':
      case 'gauge':
        return [properties.queries[0]]
      default:
        return properties.queries
    }
  }

  private get fallbackNote(): string {
    const {properties} = this.props

    switch (properties.type) {
      case 'check':
        return null
      default:
        const {note, showNoteWhenEmpty} = properties

        return showNoteWhenEmpty ? note : null
    }
  }

  private incrementSubmitToken = () => {
    this.setState({submitToken: Date.now()})
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const dashboard = state.currentDashboard.id
  const variableAssignments = getAllVariables(state, dashboard).map(v =>
    asAssignment(v)
  )
  const timeRange = getTimeRange(state, dashboard)
  const ranges = getActiveTimeRange(timeRange, ownProps.properties.queries)
  const timeZone = state.app.persisted.timeZone

  return {
    timeRange,
    ranges,
    timeZone,
    variableAssignments,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(RefreshingView)
