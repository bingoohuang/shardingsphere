/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.encrypt.rewrite.context;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import lombok.val;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptBinaryCondition;
import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewriterBuilder;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaDataAware;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.subquery.SubqueryExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;

/**
 * SQL rewrite context decorator for encrypt.
 */
public final class EncryptSQLRewriteContextDecorator implements SQLRewriteContextDecorator<EncryptRule> {

    @Override
    public void decorate(final EncryptRule encryptRule, final ConfigurationProperties props, final SQLRewriteContext sqlRewriteContext, final RouteContext routeContext) {
        SQLStatementContext stmtCtl = sqlRewriteContext.getSqlStatementContext();
        if (!containsEncryptTable(encryptRule, stmtCtl)) {
            return;
        }

        final ArrayList<EncryptCondition> encryptConditions = Lists.newArrayList();

        if (stmtCtl instanceof UpdateStatementContext) {
            val setAssignment = ((UpdateStatementContext)stmtCtl).getSqlStatement().getSetAssignment();
            for (val assignmentSegment : setAssignment.getAssignments()) {
                val v = assignmentSegment.getValue();
                if (v instanceof  SubqueryExpressionSegment) {
                    processSubQuery(encryptRule, sqlRewriteContext, (SubqueryExpressionSegment)v, stmtCtl, encryptConditions);
                }
            }
        }

        if (stmtCtl instanceof WhereAvailable) {
            createEncryptConditions(encryptRule, sqlRewriteContext,  (WhereAvailable)stmtCtl).forEach(c -> {
                if ((c instanceof EncryptBinaryCondition && ((EncryptBinaryCondition)c).getSubQuery() != null)) {
                    processSubQuery(encryptRule, sqlRewriteContext, ((EncryptBinaryCondition) c).getSubQuery(), stmtCtl, encryptConditions);
                } else{
                    encryptConditions.add(c);
                }
            });
        }

        String databaseName = sqlRewriteContext.getDatabase().getName();
        if (!sqlRewriteContext.getParameters().isEmpty()) {
            Collection<ParameterRewriter> parameterRewriters =
                    new EncryptParameterRewriterBuilder(encryptRule, databaseName, sqlRewriteContext.getDatabase().getSchemas(), stmtCtl, encryptConditions).getParameterRewriters();
            rewriteParameters(sqlRewriteContext, parameterRewriters);
        }
        Collection<SQLTokenGenerator> sqlTokenGenerators = new EncryptTokenGenerateBuilder(encryptRule, stmtCtl, encryptConditions, databaseName).getSQLTokenGenerators();
        sqlRewriteContext.addSQLTokenGenerators(sqlTokenGenerators);
    }

    private static void processSubQuery(EncryptRule encryptRule, SQLRewriteContext sqlRewriteContext, SubqueryExpressionSegment subQuery, SQLStatementContext stmtCtx, Collection<EncryptCondition> objects) {
        SelectStatement select = subQuery.getSubquery().getSelect();
        if (select.getWhere().isPresent() && stmtCtx instanceof ShardingSphereMetaDataAware) {
            ShardingSphereMetaDataAware meta = (ShardingSphereMetaDataAware) stmtCtx;
            val sc = new SelectStatementContext(meta.getShardingSphereMetaData(), null,select,  sqlRewriteContext.getDatabase().getName());

            WhereSegment ws = select.getWhere().get();
            val db = sqlRewriteContext.getDatabase();
            val engine = new EncryptConditionEngine(encryptRule, db.getSchemas());
            Collection<EncryptCondition> subConditions = engine.createEncryptConditions(Lists.newArrayList(ws),  sc.getColumnSegments(), sc, db.getName());
            objects.addAll(subConditions);
        }
    }

    private Collection<EncryptCondition> createEncryptConditions(final EncryptRule encryptRule, final SQLRewriteContext sqlRewriteContext, WhereAvailable sqlStatementContext) {
        Collection<WhereSegment> whereSegments = sqlStatementContext.getWhereSegments();
        Collection<ColumnSegment> columnSegments = sqlStatementContext.getColumnSegments();
        ShardingSphereDatabase database = sqlRewriteContext.getDatabase();
        EncryptConditionEngine engine = new EncryptConditionEngine(encryptRule, database.getSchemas());
        return engine.createEncryptConditions(whereSegments, columnSegments, (SQLStatementContext) sqlStatementContext, database.getName());
    }


    private boolean containsEncryptTable(final EncryptRule encryptRule, final SQLStatementContext sqlStatementContext) {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            if (encryptRule.findEncryptTable(each).isPresent()) {
                return true;
            }
        }
        return false;
    }

    private void rewriteParameters(final SQLRewriteContext sqlRewriteContext, final Collection<ParameterRewriter> parameterRewriters) {
        for (ParameterRewriter each : parameterRewriters) {
            each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
        }
    }

    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }

    @Override
    public Class<EncryptRule> getTypeClass() {
        return EncryptRule.class;
    }
}
