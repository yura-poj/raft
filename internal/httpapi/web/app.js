const nodes = document.querySelector('#nodes');
const observer = document.querySelector('#observer');
const updated = document.querySelector('#updated');
const warning = document.querySelector('#warning');

function text(tag, className, value) {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = value;
  return element;
}

function render(data) {
  observer.textContent = data.observer_id;
  updated.textContent = new Date(data.observed_at).toLocaleTimeString('ru-RU');
  nodes.replaceChildren();

  for (const node of data.nodes) {
    const status = node.status;
    const role = node.reachable && status ? status.role : 'unreachable';
    const card = document.createElement('article');
    card.className = `node node--${role}`;

    const heading = document.createElement('div');
    heading.className = 'node__heading';
    heading.append(text('h2', '', node.node_id));
    heading.append(text('span', `badge badge--${role}`, role));
    card.append(heading);
    card.append(text('p', 'address', node.address));

    const facts = document.createElement('dl');
    const values = [
      ['Доступен', node.reachable ? 'да' : 'нет'],
      ['Term', status ? String(status.term) : '—'],
      ['Leader', status && status.leader_id ? status.leader_id : '—'],
      ['Голос', status && status.voted_for ? status.voted_for : '—'],
    ];
    for (const [label, value] of values) {
      facts.append(text('dt', '', label));
      facts.append(text('dd', '', value));
    }
    card.append(facts);
    nodes.append(card);
  }
}

async function refresh() {
  try {
    const response = await fetch('/cluster/status', {cache: 'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
    warning.hidden = true;
  } catch (_) {
    warning.hidden = false;
  }
}

refresh();
setInterval(refresh, 1000);
