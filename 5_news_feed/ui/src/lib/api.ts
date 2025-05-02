import { User, Post, Comment, Feed, UserStats } from './types';

const API_BASE_URL = "http://localhost:8000/api";

// Helper function to handle API response
async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

// User API calls
export async function getUsers(): Promise<User[]> {
  const response = await fetch(`${API_BASE_URL}/users`);
  return handleResponse<User[]>(response);
}

export async function getUserById(userId: string): Promise<User> {
  const response = await fetch(`${API_BASE_URL}/users/${userId}`);
  return handleResponse<User>(response);
}

export async function getUserByUsername(username: string): Promise<User> {
  const response = await fetch(`${API_BASE_URL}/users/by-username/${username}`);
  return handleResponse<User>(response);
}

// Feed API calls
export async function getUserFeed(userId: string): Promise<Feed> {
  const response = await fetch(`${API_BASE_URL}/feed`, {
    headers: {
      'X-User-ID': userId
    }
  });
  return handleResponse<Feed>(response);
}

// Posts API calls
export async function getUserPosts(userId: string): Promise<Post[]> {
  const response = await fetch(`${API_BASE_URL}/users/${userId}/posts`);
  const data = await handleResponse<{posts: Post[], limit: number, offset: number, count: number}>(response);
  return data.posts;
}

export async function createPost(userId: string, content: string, postType: string = 'TEXT'): Promise<Post> {
  const response = await fetch(`${API_BASE_URL}/posts`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-User-ID': userId
    },
    body: JSON.stringify({
      content,
      post_type: postType
    })
  });
  return handleResponse<Post>(response);
}

export async function likePost(userId: string, postId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/like`, {
    method: 'POST',
    headers: {
      'X-User-ID': userId
    }
  });
  return handleResponse<void>(response);
}

export async function unlikePost(userId: string, postId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/like`, {
    method: 'DELETE',
    headers: {
      'X-User-ID': userId
    }
  });
  return handleResponse<void>(response);
}

export async function commentOnPost(userId: string, postId: string, content: string): Promise<Comment> {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/comment`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-User-ID': userId
    },
    body: JSON.stringify({
      content
    })
  });
  return handleResponse<Comment>(response);
}

export async function sharePost(userId: string, postId: string, content: string): Promise<Post> {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/share`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-User-ID': userId
    },
    body: JSON.stringify({
      content
    })
  });
  return handleResponse<Post>(response);
}

// Follower/Following API calls
export async function getFollowers(userId: string): Promise<User[]> {
  const response = await fetch(`${API_BASE_URL}/users/${userId}/followers`);
  const data = await handleResponse<{ users: User[], count: number }>(response);
  return data.users;
}

export async function getFollowing(userId: string): Promise<User[]> {
  const response = await fetch(`${API_BASE_URL}/users/${userId}/following`);
  const data = await handleResponse<{ users: User[], count: number }>(response);
  return data.users;
}

export async function followUser(userId: string, targetUserId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/users/${targetUserId}/follow`, {
    method: 'POST',
    headers: {
      'X-User-ID': userId
    }
  });
  return handleResponse<void>(response);
}

export async function unfollowUser(userId: string, targetUserId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/users/${targetUserId}/unfollow`, {
    method: 'POST',
    headers: {
      'X-User-ID': userId
    }
  });
  return handleResponse<void>(response);
}

// User stats
export async function getUserStats(userId: string): Promise<UserStats> {
  const response = await fetch(`${API_BASE_URL}/users/${userId}/stats`);
  return handleResponse<UserStats>(response);
}

// Comments API calls
export async function getPostComments(postId: string): Promise<Comment[]> {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/comments`);
  return handleResponse<Comment[]>(response);
} 