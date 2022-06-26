package org.mengyun.tcctransaction.repository;

import org.apache.commons.lang3.StringUtils;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.serializer.RegisterableKryoTransactionSerializer;
import org.mengyun.tcctransaction.serializer.TransactionSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
// File 事务存储器，将 Transaction 存储到文件系统
public class FileSystemTransactionRepository extends AbstractTransactionRepository {

    static final Logger log = LoggerFactory.getLogger(FileSystemTransactionRepository.class.getSimpleName());

    private static final String FILE_NAME_DELIMITER = "&";
    private static final String FILE_NAME_PATTERN = "*-*-*-*-*" + FILE_NAME_DELIMITER + "*-*-*-*-*";

    private String domain = "/var/log/";

    private String rootDomain = "/var/log";

    private volatile boolean initialized;

    @Override
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public String getRootDomain() {
        return rootDomain;
    }

    public void setRootDomain(String rootDomain) {
        this.rootDomain = rootDomain;
    }

    private TransactionSerializer serializer = new RegisterableKryoTransactionSerializer();

    @Override
    protected int doCreate(Transaction transaction) {
        writeFile(transaction); // 事务写入到文件
        return 1;
    }

    @Override
    protected int doUpdate(Transaction transaction) {

        Transaction foundTransaction = doFindOne(transaction.getXid()); // 从文件目录下读取事务
        if (foundTransaction.getVersion() != transaction.getVersion()) { // 传入的 version 和 读取到的要一致
            return 0;
        }

        transaction.setVersion(transaction.getVersion() + 1); // version + 1
        transaction.setLastUpdateTime(new Date());

        writeFile(transaction); // 再次将事务写入到文件
        return 1;
    }

    @Override
    protected int doDelete(Transaction transaction) {
        String fullFileName = getFullFileName(transaction.getXid()); // 获取文件事务存储器名称
        File file = new File(fullFileName);
        if (file.exists()) {
            return file.delete() ? 1 : 0; // 删除文件
        }
        return 1;
    }
    // 从文件中读取事务
    @Override
    protected Transaction doFindOne(Xid xid) {
        return doFind(domain,xid);
    }

    @Override
    protected Transaction doFindRootOne(Xid xid) {
        return doFind(rootDomain,xid);
    }

    @Override
    protected Page<Transaction> doFindAllUnmodifiedSince(Date date, String offset, int pageSize) {

        List<Transaction> fetchedTransactions = new ArrayList<>();

        String tryFetchOffset = offset;

        int haveFetchedCount = 0;

        do {
            // 按页查询
            Page<Transaction> page = doFindAll(tryFetchOffset, pageSize - haveFetchedCount);

            tryFetchOffset = page.getNextOffset();

            for (Transaction transaction : page.getData()) {
                if (transaction.getLastUpdateTime().compareTo(date) < 0) { // 按时间过滤，需要早于 date
                    fetchedTransactions.add(transaction);
                }
            }

            haveFetchedCount += page.getData().size();

            if (page.getData().size() <= 0 || haveFetchedCount >= pageSize) {
                break;
            }
        } while (true);


        return new Page<Transaction>(tryFetchOffset, fetchedTransactions);
    }
    // 从文件目录下读取事务
    private Transaction doFind(String domain,Xid xid) {
        makeDirIfNecessary(domain); // 创建文件目录
        // 获取文件事务存储器名称
        String fullFileName = getFullFileName(xid);
        File file = new File(fullFileName);
        // 文件存在就读取
        if (file.exists()) {
            return readTransaction(file);
        }

        return null;
    }

    /*
     * offset: jedisIndex:cursor,eg = 0:0,1:0
     * */
    private Page<Transaction> doFindAll(String offset, int maxFindCount) {

        makeDirIfNecessary(domain); // 创建文件目录

        Page<Transaction> page = new Page<Transaction>();

        Path dir = Paths.get(domain);

        int currentOffset = StringUtils.isEmpty(offset) ? 0 : Integer.valueOf(offset);
        int nextOffset = currentOffset;

        int index = 0;

        List<Transaction> transactions = new ArrayList<Transaction>();
        // 获取目录下所有符合 FILE_NAME_PATTERN 命名的文件
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, FILE_NAME_PATTERN)) {

            for (Path path : stream) {

                if (index < currentOffset) { // 前进到 currentOffset 偏移处开始读取
                    index++;
                    continue;
                }

                if (index < currentOffset + maxFindCount) { // 循环读取

                    try {
                        // 从文件中读取一个事务
                        Transaction transaction = readTransaction(path.toFile());
                        if (transaction != null) {
                            transactions.add(transaction);
                        }

                    } catch (Exception e) {
                        // ignore the read error.
                    }

                    index++;
                    nextOffset = index; // 更新 nextOffset
                } else {
                    break;
                }
            }
            // 返回下一页要读取的偏移量
            page.setNextOffset(String.valueOf(nextOffset));
            page.setData(transactions);

            return page;

        } catch (IOException e) {
            throw new TransactionIOException(e);
        }
    }

    private void writeFile(Transaction transaction) {
        makeDirIfNecessary(domain); // 创建文件目录
        // 获取文件事务存储器名称
        String file = getFullFileName(transaction.getXid());

        FileChannel channel = null;
        RandomAccessFile raf = null;
        // 序列化
        byte[] content = serializer.serialize(transaction);
        try {
            raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(content.length);
            buffer.put(content);
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer); // nio write
            }

            channel.force(true);
        } catch (Exception e) {
            throw new TransactionIOException(e);
        } finally {
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new TransactionIOException(e);
                }
            }
        }
    }
    // 从文件中读取事务
    private Transaction readTransaction(File file) {

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);

            byte[] content = new byte[(int) file.length()];

            fis.read(content);

            if (content != null) {
                return serializer.deserialize(content); // 反序列化
            }

        } catch (Exception e) {
            throw new TransactionIOException(e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new TransactionIOException(e);
                }
            }
        }

        return null;
    }
    // DCL，创建文件目录
    private void makeDirIfNecessary(String domain) {
        if (!initialized) {
            synchronized (FileSystemTransactionRepository.class) {
                if (!initialized) {
                    File rootPathFile = new File(domain);
                    if (!rootPathFile.exists()) {

                        boolean result = rootPathFile.mkdir();

                        if (!result) {
                            throw new TransactionIOException("cannot create root path, the path to create is:" + domain);
                        }

                        initialized = true;
                    } else if (!rootPathFile.isDirectory()) {
                        throw new TransactionIOException("rootPath is not directory");
                    }
                }
            }
        }
    }
    // 格式：domainGlobalTransactionId&branchQualifier   /var/log/0be50a44-7589-360e-9559-f1b8358fde51&b5a83387-bb1a-3a27-ba5b-83cf556640ff
    private String getFullFileName(Xid xid) {
        return String.format(domain.endsWith("/") ? "%s%s" + FILE_NAME_DELIMITER + "%s" : "%s/%s" + FILE_NAME_DELIMITER + "%s", domain,
                UUID.nameUUIDFromBytes(xid.getGlobalTransactionId()).toString(),
                UUID.nameUUIDFromBytes(xid.getBranchQualifier()).toString());
    }
}
