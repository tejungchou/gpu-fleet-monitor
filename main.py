import datetime
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


class FakeServer:
    def __init__(self, server_id, rack_id, num_gpus=4):
        self.server_id = server_id
        self.rack_id = rack_id
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



def main():
    server = FakeServer('server-1', 'rack-1', 4)
    agent = FakeAgent(server)
    report = agent.collect()
    print(f"Original state: {report}\n")
    load_changes = ['increase', 'increase', 'decrease', 'decrease']
    for i, change in enumerate(load_changes):
        for gpu in server.gpus:
            gpu.update(change)
        print("--------------------------------------")
        print(f"After change {i}: {agent.collect()}\n")


if __name__ == "__main__":
    main()