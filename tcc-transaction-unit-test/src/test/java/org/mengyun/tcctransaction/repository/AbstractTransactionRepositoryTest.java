package org.mengyun.tcctransaction.repository;

import org.junit.After;
import org.junit.Before;
import org.mengyun.tcctransaction.api.TransactionXid;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * AbstractTransactionRepositoryTest
 *
 * @author <a href="kangjinghang@xueqiu.com">kangjinghang</a>
 * @date 2022-06-26
 * @since 1.0.0
 */
public abstract class AbstractTransactionRepositoryTest {

    protected TransactionRepository repository;
    protected UUID rootGlobalUUID;
    protected UUID rootBranchUUID;
    protected UUID globalUUID;
    protected UUID branchUUID;

    protected TransactionXid xid;
    protected TransactionXid rootXid;

    @Before
    public void setUp() {
        repository = doCreateTransactionRepository();
        rootGlobalUUID = UUID.fromString("59237939-dae0-387d-b3bb-e11933ce52d0");
        rootBranchUUID = UUID.fromString("2657434a-0d43-3f53-aef3-3375dbe424b9");
        globalUUID = UUID.fromString("d43f8a17-39e8-35b6-9c96-5ea224926458");
        branchUUID = UUID.fromString("20c10fee-6963-3f40-95aa-c53f6c0e2baf");
        xid = new TransactionXid(uuidToByteArray(globalUUID), uuidToByteArray(branchUUID));
        rootXid = new TransactionXid(uuidToByteArray(rootGlobalUUID), uuidToByteArray(rootBranchUUID));
    }

    abstract TransactionRepository doCreateTransactionRepository();

    @After
    public void destroy() {
        repository.close();
    }

    protected static byte[] uuidToByteArray(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

}
