import datetime
import time
class FakeGPU:
    def __init__(self, gpu_id, temperature, power, utilization, ecc_single_bit_errors, ecc_double_bit_errors, xid_errors ,nvlink_errors):
        self.gpu_id = gpu_id
        self.temperature = temperature
        self.power = power
        self.utilization = utilization
        self.ecc_single_bit_errors = ecc_single_bit_errors
        self.ecc_double_bit_errors = ecc_double_bit_errors
        self.xid_errors = xid_errors
        self.nvlink_errors = nvlink_errors

    def update(self, load):
        if load.lower() == 'increase':
            self.utilization += 10
        elif load.lower() == 'decrease':
            self.utilization -= 10
        

        # GPU utilization should always be 0% - 100%
        if self.utilization >= 100:
            self.utilization = 100
        if self.utilization <= 0:
            self.utilization = 0
        # Based on this map: 
        #   0 % GPU utilization -> 40 C
        # 100 % GPU utilization -> 90 C
        # T = 0.5 U + 40
        self.temperature = 0.5 * self.utilization + 40

    def inject_ecc_error(self, error_type):
        if error_type == 'double':
            self.ecc_double_bit_errors += 1

        elif error_type == 'single':
            self.ecc_single_bit_errors += 1

class FakeServer:
    def __init__(self, server_id, rack_id, num_gpus=4):
        self.server_id = server_id
        self.rack_id = rack_id
        self.health = 'HEALTHY'
        self.gpus = []
        for gpu_id in range(num_gpus):
            gpu = FakeGPU(gpu_id, 62.5, 1400, 45, 0, 0, 0, 0)
            self.gpus.append(gpu)


class FakeAgent:
    def __init__(self, server):
        self.server = server
    def collect(self):
        collection_list = []
        collection_time = datetime.datetime.now()

        for gpu in self.server.gpus:
            gpu_data = {}
            gpu_data['rack_id'] = self.server.rack_id
            gpu_data['server_id'] = self.server.server_id
            gpu_data['gpu_id'] = gpu.gpu_id
            gpu_data['gpu_temperature'] = gpu.temperature
            gpu_data['gpu_power'] = gpu.power
            gpu_data['gpu_utilization'] = gpu.utilization
            gpu_data['gpu_ecc_single_bit_errors'] = gpu.ecc_single_bit_errors
            gpu_data['gpu_ecc_double_bit_errors'] = gpu.ecc_double_bit_errors
            gpu_data['gpu_xid_errors'] = gpu.xid_errors
            gpu_data['gpu_nvlink_errors'] = gpu.nvlink_errors
            gpu_data['collect_time'] = collection_time
            collection_list.append(gpu_data)

        return collection_list

class FakeObserver:
    def __init__(self, rules):
        self.rules = rules
        self.previous_telemetry = {}

    def observe(self, telemetry):
        issues = []

        for gpu in telemetry:
            key = (gpu['server_id'], gpu['gpu_id'])

            previous_gpu = self.previous_telemetry.get(key)

            for rule in self.rules:
                result = rule.check(gpu, previous_gpu)

                if result:
                    issues.append({
                        'rack_id': gpu['rack_id'],
                        'server_id': gpu['server_id'],
                        'gpu_id': gpu['gpu_id'],
                        'issue_type': result['issue_type'],
                        'severity': result['severity'],
                        'detect_time': gpu['collect_time']
                    })
            self.previous_telemetry[key] = gpu 
        return issues

    def group_by_server(self, telemetry):
        servers = {}

        for gpu in telemetry:
            server_id = gpu['server_id']

            if server_id not in servers:
                servers[server_id] = []

            servers[server_id].append(gpu)
        return servers
        
    def group_issues_by_server(self, issues):
        servers = {}

        for issue in issues:
            server_id = issue['server_id']

            if server_id not in servers:
                servers[server_id] = []

            servers[server_id].append(issue)
        return servers

class ServerHealthRule:
    def check(self, issues, current_health):
        critical = False
        degraded = False

        if current_health == 'CRITICAL':
            return 'CRITICAL'
        
        for issue in issues:
            if issue['severity'] == 'CRITICAL':
                critical = True
            elif issue['severity'] == 'WARNING':
                degraded = True

        if critical:
            return 'CRITICAL'
        elif degraded:
            return 'DEGRADED'
        else:
            return 'HEALTHY'

class TemperatureRule:
    def check(self, gpu, previous_gpu=None):

        temperature = gpu['gpu_temperature']

        if temperature <= 85:
            return None
        
        elif 85 < temperature < 90:
            return {
                'issue_type': 'HIGH_TEMPERATURE',
                'severity': 'WARNING'
            }
        
        else:
            return {
                'issue_type': 'HIGH_TEMPERATURE',
                'severity': 'CRITICAL'
            }   
        
class ECCRule:
    def check(self, gpu, previous_gpu):
        if not previous_gpu:
            prev_ecc_error_single = 0
            prev_ecc_error_double = 0
        else:
            prev_ecc_error_single = previous_gpu['gpu_ecc_single_bit_errors']
            prev_ecc_error_double = previous_gpu['gpu_ecc_double_bit_errors']


        ecc_error_single = gpu['gpu_ecc_single_bit_errors']
        ecc_error_double = gpu['gpu_ecc_double_bit_errors']

        if ecc_error_double > prev_ecc_error_double:
            return {
                'issue_type': 'UNCORRECTABLE_ERROR',
                'severity': 'CRITICAL'
            }
        elif ecc_error_single > prev_ecc_error_single:
            return {
                'issue_type': 'CORRECTABLE_ERROR',
                'severity': 'WARNING'
            }
        else:
            return None

class FleetSimulator:
    def run(self, num_servers=1):
        fleet = []
        for num in range(num_servers):
            server = FakeServer(
                server_id=f"server-{num}",
                rack_id=f"rack-{num // 10}"
            )
            agent = FakeAgent(server)
            fleet.append((server, agent))

        return fleet
    
    def collect(self, fleet):
        collection = []
        for server, agent in fleet:
            telemetry = agent.collect()
            collection.extend(telemetry)
        
        return collection


def main():
    simulator = FleetSimulator()
    observer = FakeObserver([
        TemperatureRule(),
        ECCRule()
    ])

    fleet = simulator.run(num_servers=3)
    health_rules = ServerHealthRule()

    for i in range(5):

        # 1. Update entire fleet
        for server, agent in fleet:
            for gpu in server.gpus:
                gpu.update('increase')
                # inject ecc error
                if i == 2 and gpu.gpu_id == 3:
                    gpu.inject_ecc_error('double')
            
        # 2. Collect entire fleet once
        collection = simulator.collect(fleet)

        # 3. Observe entire fleet once
        issues = observer.observe(collection)

        # 4. Group issues
        grouped_issues = observer.group_issues_by_server(issues)

        # 5. Calculate server health
        for server, agent in fleet:
            server_issues = grouped_issues.get(server.server_id, [])
            health = health_rules.check(server_issues, server.health)

            server.health = health
            print(server.server_id, server.health)


        time.sleep(2)

    

if __name__ == "__main__":
    main()