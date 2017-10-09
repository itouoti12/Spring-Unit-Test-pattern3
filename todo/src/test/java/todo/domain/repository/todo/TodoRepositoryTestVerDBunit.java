package todo.domain.repository.todo;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.dbunit.Assertion;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import todo.domain.model.Todo;

/**
 * Repository Test
 * pring-test-dbunitによるデータのセットアップ、比較
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:META-INF/spring/test-context-dbunit.xml"})
@Transactional
public class TodoRepositoryTestVerDBunit extends DataSourceBasedDBTestCase {

	private final String RESOURCE_DIR = "src/test/resources/META-INF/dbunit/";
	
	@Inject
	TodoRepository target;
	
	@Inject
	JdbcTemplate jdbctemplate;
	
	@Inject
	private TransactionAwareDataSourceProxy dataSourceTest;
	
	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		/* 意味ないかもしれないけど、ここで実行することでファイルとＤＢの格納後の整合性確認ができる。
		IDataSet databaseDataSet = getConnection().createDataSet();
		ITable actualTable = databaseDataSet.getTable("todo");
		
		
		IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(new File(RESOURCE_DIR + "test_data.xml"));
		ITable expectedTable = expectedDataSet.getTable("todo");
		
		Assertion.assertEquals(expectedTable, actualTable);
		*/
		
	}
	
	@Test
	@Rollback
	public void testUpdate() throws Exception {
		//テスト用のデータを作成
		String todoId = "cceae402-c5b1-440f-bae2-7bee19dc17fb";
		Todo testDataTodo = getTodoData(todoId);
		testDataTodo.setFinished(true);
		
		//updateメソッドのテスト
		boolean actTodo = target.update(testDataTodo);
		
		//結果検証
		assertEquals(actTodo, true);
		
		//メソッド実行後テーブルデータ検証
		//ここで指定したxmlファイルには、テスト対象のメソッド実行後に更新されたテーブルの内容の期待値を記述している
		Assertion.assertEquals(getexpectedTable(RESOURCE_DIR + "compare_data.xml"), getactualTable());
		
	}
	
	//テーブルからデータを取得
	private ITable getactualTable() throws Exception {
		
		IDataSet databaseDataSet = getConnection().createDataSet();
		ITable actualTable = databaseDataSet.getTable("todo");
		
		return actualTable;
	}
	
	//期待値となるデータをファイルから取得するメソッド。引数はファイルのパス
	private ITable getexpectedTable(String filepath) throws Exception  {
		
		IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(new File(filepath));
		ITable expectedTable = expectedDataSet.getTable("todo");
		
		return expectedTable;
	}
	

	//DBunitを使用するためのオーバーライド
	@Override
	protected DataSource getDataSource() {
		// TODO 自動生成されたメソッド・スタブ
		return dataSourceTest;
	}

	//DBunitを使用するためのオーバーライド
	@Override
	protected IDataSet getDataSet() throws Exception {
		// TODO 自動生成されたメソッド・スタブ
		return new FlatXmlDataSetBuilder().build(new FileInputStream(RESOURCE_DIR + "test_data.xml"));
	}
	
	//テスト用元データの取得
	private Todo getTodoData(String todoId) {
		
		String sql = "SELECT * FROM todo WHERE todo_id=?";
		
		Todo todoData = (Todo)jdbctemplate.queryForObject(sql, new Object[] {todoId},
				new RowMapper<Todo>() {
					public Todo mapRow(ResultSet rs, int rownum) throws SQLException {
						Todo todoSql = new Todo();
						
						todoSql.setTodoId(rs.getString("todo_id"));
						todoSql.setTodoTitle(rs.getString("todo_title"));
						todoSql.setFinished(rs.getBoolean("finished"));
						todoSql.setCreatedAt(rs.getTimestamp("created_at"));
					
						return todoSql;
					}
		});
		return todoData;
	}

}
