package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.*;

import java.io.Serializable;

/**
 * Created by changmingxie on 10/27/15. 事务参与者
 */
public class Participant implements Serializable {

    private static final long serialVersionUID = 4127729421281425247L;
    // 事务上下文编辑
    Class<? extends TransactionContextEditor> transactionContextEditorClass;

    private TransactionXid rootXid;

    // 参与者事务编号
    private TransactionXid xid;
    private InvocationContext confirmInvocationContext; // commit 执行业务方法调用上下文
    private InvocationContext cancelInvocationContext; // rollback 执行业务方法调用上下文
    private int status = ParticipantStatus.TRYING.getId();

    public Participant() {

    }

    public Participant(TransactionXid rootXid, TransactionXid xid, InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.xid = xid;
        this.rootXid = rootXid;
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }
    // 回滚参与者自己的事务，其实就是执行 cancel 方法
    public void rollback() {
        Terminator.invoke(new TransactionContext(rootXid, xid, TransactionStatus.CANCELLING.getId(), status), cancelInvocationContext, transactionContextEditorClass);
    }
    // 提交参与者自己的事务，其实就是执行 confirm 方法
    public void commit() {
        Terminator.invoke(new TransactionContext(rootXid, xid, TransactionStatus.CONFIRMING.getId(), status), confirmInvocationContext, transactionContextEditorClass);
    }

    public InvocationContext getConfirmInvocationContext() {
        return confirmInvocationContext;
    }

    public InvocationContext getCancelInvocationContext() {
        return cancelInvocationContext;
    }

    public void setStatus(ParticipantStatus status) {
        this.status = status.getId();
    }

    public ParticipantStatus getStatus() {
        return ParticipantStatus.valueOf(this.status);
    }

    public TransactionXid getXid() {
        return xid;
    }

    public Class<? extends TransactionContextEditor> getTransactionContextEditorClass() {
        return transactionContextEditorClass;
    }

}
