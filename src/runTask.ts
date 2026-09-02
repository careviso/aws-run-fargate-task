import {
  DescribeClustersCommand,
  DescribeTasksCommand,
  ECSClient,
  KeyValuePair,
  RunTaskCommand,
  RunTaskCommandInput,
  waitUntilTasksStopped,
} from '@aws-sdk/client-ecs';
import {
  DescribeSecurityGroupsCommand,
  DescribeSubnetsCommand,
  EC2Client,
  Filter,
} from '@aws-sdk/client-ec2';
import * as core from '@actions/core';

export class ClusterNotFound extends Error {}
export class TaskCreationError extends Error {}
export class TaskSatateError extends Error {}

export type CapacityProvider = 'FARGATE' | 'FARGATE_SPOT';

interface Params {
  checkClusterExists?: boolean;
  count?: number;
  isPublicIp?: boolean;
  sgFilters?: Filter[];
  sgIds?: string[];
  sgNames?: string[];
  subnetFilters?: Filter[];
  subnetIds?: string[];
  command?: string[];
  environment?: KeyValuePair[];
  timeout?: number;
  wait?: boolean;
  pollDelay?: number;
  capacityProvider?: CapacityProvider;
}

const ecs = new ECSClient({});
const ec2 = new EC2Client({});

async function hasCluster(cluster: string) {
  const foundedClusters = await ecs.send(new DescribeClustersCommand({ clusters: [cluster] }));

  return foundedClusters.clusters?.[0]?.clusterName === cluster;
}

export default async function runTask(
  taskName: string,
  cluster: string,
  {
    checkClusterExists = false,
    isPublicIp = false,
    count = 1,
    sgFilters,
    sgIds,
    sgNames,
    subnetFilters,
    subnetIds,
    command,
    environment,
    timeout = 600,
    wait = true,
    pollDelay = 6,
    capacityProvider,
  }: Params = {},
) {
  if (checkClusterExists && !(await hasCluster(cluster))) {
    core.error(`Error: cluster "${cluster}" not found! Check out params!`);

    throw new ClusterNotFound();
  }

  const { securityGroupIds, sbnIds } = await core.group('Fetch network settings', async () => {
    const [sg, subnets] = await Promise.all([
      ec2.send(
        new DescribeSecurityGroupsCommand({
          Filters: sgFilters,
          GroupIds: sgIds,
          GroupNames: sgNames,
        }),
      ),
      ec2.send(
        new DescribeSubnetsCommand({
          Filters: subnetFilters,
          SubnetIds: subnetIds,
        }),
      ),
    ]);

    const securityGroupIds =
      sg.SecurityGroups?.map((group) => group.GroupId).filter((id): id is string => !!id) ??
      undefined;

    core.info(`SecurityGroups ids: ${!securityGroupIds ? 'empty' : securityGroupIds.join(',')}`);

    const sbnIds = (subnets.Subnets?.map((net) => net.SubnetId) ?? []).filter(
      (id): id is string => !!id,
    );

    core.info(`Sunets ids: ${!sbnIds ? 'empty' : sbnIds.join(',')}`);

    return { securityGroupIds, sbnIds };
  });

  return await core.group('Flush task to ECS', async () => {
    core.info(`Run task: ${taskName}`);

    const runTaksRequestParams: RunTaskCommandInput = {
      count,
      cluster,
      taskDefinition: taskName,
      networkConfiguration: {
        awsvpcConfiguration: {
          subnets: sbnIds,
          securityGroups: securityGroupIds,
          assignPublicIp: isPublicIp ? 'ENABLED' : 'DISABLED',
        },
      },
    };

    if (command || environment) {
      runTaksRequestParams.overrides = {
        containerOverrides: [
          {
            name: taskName,
            command,
            environment,
          },
        ],
      };
    }

    if (capacityProvider) {
      runTaksRequestParams.capacityProviderStrategy = [
        {
          base: count,
          capacityProvider,
          weight: 1,
        },
      ];
    }

    const runTaskResponse = await ecs.send(new RunTaskCommand(runTaksRequestParams));

    if (!runTaskResponse.tasks?.length || !runTaskResponse.tasks[0].taskArn) {
      console.log('Run ecs task response >>>', runTaskResponse);

      core.error(`Error: task "${taskName}" couldn't created! Check out params!`);

      throw new TaskCreationError();
    }

    if (!wait) {
      console.log('task >>>', runTaskResponse);

      return 0;
    }

    core.info('Wait unill task stopped');

    const tasks = [runTaskResponse.tasks[0].taskArn];

    await waitUntilTasksStopped(
      {
        client: ecs,
        maxWaitTime: timeout,
        // min === max pins a fixed poll interval; left unset, v3 backs off exponentially to 600s.
        minDelay: pollDelay,
        maxDelay: pollDelay,
      },
      { cluster, tasks },
    );

    core.info('Task stopped. Checkout exit state.');

    const taskState = await ecs.send(new DescribeTasksCommand({ cluster, tasks }));

    if (!taskState.tasks?.length || !taskState.tasks[0].taskArn) {
      core.error(`Error: task "${taskName}" couldn't fetch current state!`);

      throw new TaskSatateError();
    }

    console.log('task >>>', taskState);

    const exitCode = taskState.tasks[0].containers?.[0].exitCode ?? 1;
    const exitReason = taskState.tasks[0].containers?.[0].reason ?? 'Unknown';

    core.info(`Run finished. Task stopped with code "${exitCode}" and reason "${exitReason}"`);
    return exitCode;
  });
}
