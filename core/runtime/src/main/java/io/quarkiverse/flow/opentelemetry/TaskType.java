package io.quarkiverse.flow.opentelemetry;

import java.util.NoSuchElementException;

import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.DoTask;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.ForTask;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.ListenTask;
import io.serverlessworkflow.api.types.RaiseTask;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SwitchTask;
import io.serverlessworkflow.api.types.TaskBase;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.WaitTask;

public enum TaskType {
    CALL,
    DO,
    EMIT,
    FOR,
    FORK,
    LISTEN,
    RAISE,
    RUN,
    SET,
    SWITCH,
    TRY,
    WAIT;

    static TaskType fromTask(TaskBase taskBase) {
        if (taskBase.getClass() == CallHTTP.class) {
            return CALL;
        }
        if (taskBase.getClass() == ForkTask.class) {
            return FORK;
        }
        if (taskBase.getClass() == ForTask.class) {
            return FOR;
        }
        if (taskBase.getClass() == SetTask.class) {
            return SET;
        }
        if (taskBase.getClass() == DoTask.class) {
            return DO;
        }
        if (taskBase.getClass() == RunTask.class) {
            return RUN;
        }
        if (taskBase.getClass() == ListenTask.class) {
            return LISTEN;
        }
        if (taskBase.getClass() == WaitTask.class) {
            return WAIT;
        }
        if (taskBase.getClass() == EmitTask.class) {
            return EMIT;
        }
        if (taskBase.getClass() == TryTask.class) {
            return TRY;
        }
        if (taskBase.getClass() == RaiseTask.class) {
            return RAISE;
        }
        if (taskBase.getClass() == SwitchTask.class) {
            return SWITCH;
        }
        throw new NoSuchElementException("TaskBase: " + taskBase.getClass() + " is not recognized.");
    }
}
