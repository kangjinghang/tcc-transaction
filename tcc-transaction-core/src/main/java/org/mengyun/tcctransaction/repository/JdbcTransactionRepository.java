package org.mengyun.tcctransaction.repository;

import org.apache.commons.lang3.StringUtils;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.serializer.RegisterableKryoTransactionSerializer;
import org.mengyun.tcctransaction.serializer.TransactionSerializer;
import org.mengyun.tcctransaction.utils.CollectionUtils;

import javax.sql.DataSource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by changmingxie on 10/30/15. JDBC 事务存储器，通过 JDBC 驱动，将 Transaction 存储到 MySQL / Oracle / PostgreSQL / SQLServer 等关系数据库
 */
public class JdbcTransactionRepository extends AbstractTransactionRepository {
    // 领域。或者也可以称为模块名，应用名，用于唯一标识一个资源。例如，Maven 模块 xxx-order，我们可以配置该属性为 ORDER。
    private String domain;
    // 表后缀。默认存储表名为 TCC_TRANSACTION，配置表名后，为 TCC_TRANSACTION${tbSuffix}
    private String tbSuffix;

    private String rootDomain;

    private String rootTbSuffix;
    // 数据源
    private DataSource dataSource;
    // 序列化
    private TransactionSerializer serializer = new RegisterableKryoTransactionSerializer();

    @Override
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setRootDomain(String rootDomain) {
        this.rootDomain = rootDomain;
    }

    @Override
    public String getRootDomain() {
        return rootDomain;
    }

    public String getTbSuffix() {
        return tbSuffix;
    }

    public void setTbSuffix(String tbSuffix) {
        this.tbSuffix = tbSuffix;
    }

    public String getRootTbSuffix() {
        return rootTbSuffix;
    }

    public void setRootTbSuffix(String rootTbSuffix) {
        this.rootTbSuffix = rootTbSuffix;
    }

    public void setSerializer(TransactionSerializer serializer) {
        this.serializer = serializer;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    protected int doCreate(Transaction transaction) {

        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();
            // SQL
            StringBuilder builder = new StringBuilder();
            builder.append("INSERT INTO " + getTableName() +
                    "(GLOBAL_TX_ID,BRANCH_QUALIFIER,TRANSACTION_TYPE,CONTENT,STATUS,RETRIED_COUNT,CREATE_TIME,LAST_UPDATE_TIME,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN) VALUES (?,?,?,?,?,?,?,?,?,?)" : ") VALUES (?,?,?,?,?,?,?,?,?)");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setBytes(1, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(2, transaction.getXid().getBranchQualifier());
            stmt.setInt(3, transaction.getTransactionType().getId());
            stmt.setBytes(4, serializer.serialize(transaction));
            stmt.setInt(5, transaction.getStatus().getId());
            stmt.setInt(6, transaction.getRetriedCount());
            stmt.setTimestamp(7, new Timestamp(transaction.getCreateTime().getTime()));
            stmt.setTimestamp(8, new Timestamp(transaction.getLastUpdateTime().getTime()));
            stmt.setLong(9, transaction.getVersion());

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(10, domain);
            }
            // 执行
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    @Override
    protected int doUpdate(Transaction transaction) {
        Connection connection = null;
        PreparedStatement stmt = null;

        Date lastUpdateTime = transaction.getLastUpdateTime();
        long currentVersion = transaction.getVersion();
        // 设置最后更新时间 和 最新版本号
        transaction.setLastUpdateTime(new Date());
        transaction.setVersion(transaction.getVersion() + 1);

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("UPDATE " + getTableName() + " SET " +
                    "CONTENT = ?,STATUS = ?,LAST_UPDATE_TIME = ?, RETRIED_COUNT = ?,VERSION = VERSION+1 WHERE GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ? AND VERSION = ?");

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setBytes(1, serializer.serialize(transaction));
            stmt.setInt(2, transaction.getStatus().getId());
            stmt.setTimestamp(3, new Timestamp(transaction.getLastUpdateTime().getTime()));

            stmt.setInt(4, transaction.getRetriedCount());
            stmt.setBytes(5, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(6, transaction.getXid().getBranchQualifier());
            stmt.setLong(7, currentVersion);

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(8, domain);
            }
            // 执行
            int result = stmt.executeUpdate();

            return result;

        } catch (Throwable e) {
            transaction.setLastUpdateTime(lastUpdateTime);
            transaction.setVersion(currentVersion);
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    @Override
    protected int doDelete(Transaction transaction) {
        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();
            // SQL
            StringBuilder builder = new StringBuilder();
            builder.append("DELETE FROM " + getTableName() +
                    " WHERE GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ?");

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setBytes(1, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(2, transaction.getXid().getBranchQualifier());

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(3, domain);
            }
            // 执行
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    @Override
    protected Transaction doFindOne(Xid xid) {
        return doFind(getDomain(),getTableName(),xid);
    }

    @Override
    protected Transaction doFindRootOne(Xid xid) {
        return doFind(getRootDomain(),getRootTableName(),xid);
    }

    @Override
    protected Page<Transaction> doFindAllUnmodifiedSince(Date date, String offset, int pageSize) {

        List<Transaction> transactions = new ArrayList<Transaction>();

        Connection connection = null;
        PreparedStatement stmt = null;

        int currentOffset = StringUtils.isEmpty(offset) ? 0 : Integer.valueOf(offset);

        try {
            connection = this.getConnection();
            // SQL
            StringBuilder builder = new StringBuilder();

            builder.append("SELECT GLOBAL_TX_ID, BRANCH_QUALIFIER, CONTENT,STATUS,TRANSACTION_TYPE,CREATE_TIME,LAST_UPDATE_TIME,RETRIED_COUNT,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN" : "");
            builder.append("  FROM " + getTableName() + " WHERE LAST_UPDATE_TIME < ?");  // 最后更新时间
            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");
            builder.append(" ORDER BY TRANSACTION_ID ASC");
            builder.append(String.format(" LIMIT %s, %d", currentOffset, pageSize));

            stmt = connection.prepareStatement(builder.toString());

            stmt.setTimestamp(1, new Timestamp(date.getTime()));

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(2, domain);
            }
            // 执行
            ResultSet resultSet = stmt.executeQuery();
            // 创建 Transaction
            this.constructTransactions(resultSet, transactions);
        } catch (Throwable e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }

        return new Page<Transaction>(String.valueOf(currentOffset + transactions.size()), transactions);
    }

    private Transaction doFind(String domain, String tableName,Xid xid) {

        List<Transaction> transactions = doFinds(domain,tableName,Arrays.asList(xid));

        if (!CollectionUtils.isEmpty(transactions)) {
            return transactions.get(0);
        }
        return null;
    }

    private List<Transaction> doFinds(String domain, String tableName,List<Xid> xids) {

        List<Transaction> transactions = new ArrayList<Transaction>();

        if (CollectionUtils.isEmpty(xids)) {
            return transactions;
        }

        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();
            // SQL
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT GLOBAL_TX_ID, BRANCH_QUALIFIER, CONTENT,STATUS,TRANSACTION_TYPE,CREATE_TIME,LAST_UPDATE_TIME,RETRIED_COUNT,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN" : "");
            builder.append("  FROM " + getTableName() + " WHERE");

            if (!CollectionUtils.isEmpty(xids)) {
                for (Xid xid : xids) {
                    builder.append(" ( GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ? ) OR");  // 通过 or 拼接多个 GLOBAL_TX_ID + BRANCH_QUALIFIER 组合
                }

                builder.delete(builder.length() - 2, builder.length());
            }

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            int i = 0;

            for (Xid xid : xids) {
                stmt.setBytes(++i, xid.getGlobalTransactionId());
                stmt.setBytes(++i, xid.getBranchQualifier());
            }

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(++i, domain);
            }
            // 执行
            ResultSet resultSet = stmt.executeQuery();
            // 创建 Transaction
            this.constructTransactions(resultSet, transactions);
        } catch (Throwable e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }

        return transactions;
    }
    // 创建 Transaction 集合
    private void constructTransactions(ResultSet resultSet, List<Transaction> transactions) throws SQLException {
        while (resultSet.next()) {
            byte[] transactionBytes = resultSet.getBytes(3);
            Transaction transaction = (Transaction) serializer.deserialize(transactionBytes);
            transaction.setStatus(TransactionStatus.valueOf(resultSet.getInt(4)));
            transaction.setLastUpdateTime(resultSet.getDate(7));
            transaction.setVersion(resultSet.getLong(9));
            transaction.setRetriedCount(resultSet.getInt(8));
            transactions.add(transaction);
        }
    }
    // 获取 Connection
    private Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        }
    }
    // 释放 Connection
    private void releaseConnection(Connection con) {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        }
    }
    // 释放 Statement
    private void closeStatement(Statement stmt) {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
            }
        } catch (Exception ex) {
            throw new TransactionIOException(ex);
        }
    }

    private String getTableName() {
        return StringUtils.isNotEmpty(tbSuffix) ? "AGG_TRANSACTION_" + tbSuffix : "AGG_TRANSACTION";
    }


    private String getRootTableName() {
        return StringUtils.isNotEmpty(rootTbSuffix) ? "AGG_TRANSACTION_" + rootTbSuffix : "AGG_TRANSACTION";
    }

}
