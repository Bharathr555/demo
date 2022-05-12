package test;



import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.onwbp.adaptation.*;
import com.orchestranetworks.scheduler.ScheduledExecutionContext;
import com.orchestranetworks.scheduler.ScheduledTask;
import com.orchestranetworks.scheduler.ScheduledTaskInterruption;
import com.orchestranetworks.schema.Path;
import com.onwbp.base.repository.*;
import com.onwbp.com.google.gson.JsonArray;
import com.onwbp.com.google.gson.JsonElement;
import com.onwbp.com.google.gson.JsonObject;
import com.onwbp.com.google.gson.JsonParser;
import com.orchestranetworks.instance.Repository;
import com.orchestranetworks.instance.HomeKey;
import com.orchestranetworks.service.*;

public class EmployeeSheduledTaskProcudre implements Procedure {

        static ProcedureContext trnsctx;
        Repository repo;
        Session session;
        AdaptationHome dataspace;
        static HashMap<String, String> empMap = new HashMap<>();
        static HashMap<String, String> empMapjson = new HashMap<>();

        public EmployeeSheduledTaskProcudre(Repository repo, Session session, AdaptationHome dataspace) {
                this.session = session;
                this.repo = repo;
                this.dataspace = dataspace;
        }

        @Override
        public void execute(ProcedureContext procedureContext) throws Exception {
                System.out.println("In  EmployeeSheduledTaskProcudre execute");
                trnsctx = procedureContext;
                AdaptationHome dataSpace = repo.lookupHome(HomeKey.forBranchName("Reference"));
                Adaptation dataSet = dataSpace.findAdaptationOrNull(AdaptationName.forName("hgx"));
                Path path = Path.parse("/root/emp");
                Path c1 = path.parse("./id");
                Path c2 = path.parse("./name");
                AdaptationTable empTable = dataSet.getTable(path);
                ValueContextForUpdate valueContext = procedureContext.getContextForNewOccurrence(empTable);
                MessageReceiver ms = new MessageReceiver();
               
	                empMapjson = ms.receive("Newtopic");
	                for (Entry<String, String> entry : empMapjson.entrySet()) {
						String key = entry.getKey();
						String val = entry.getValue();
					
	                       Object object=null;
	                      
	                       JsonParser jsonParser=new JsonParser();
	                       object=jsonParser.parse(val);
	                       JsonElement arrayObj=(JsonElement) object;
	                       JsonObject json = arrayObj.getAsJsonObject();
	                       
	                               empMap.put("id", json.get("id").getAsString());
	                                   empMap.put("name",  json.get("name").getAsString());
	                                   
	                                   valueContext.setValue(empMap.get("id"), c1);
	                                valueContext.setValue(empMap.get("name"), c2);
	                                Adaptation record = procedureContext.doCreateOccurrence(valueContext, empTable);
	                }
                      
                }

                
               

        }

